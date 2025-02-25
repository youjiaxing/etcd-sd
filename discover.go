package sd

import (
	"context"
	"encoding/json"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"sync"
	"time"
)

func NewDiscover(endPoints []string) (*Discover, error) {
	ctx, cancel := context.WithCancel(context.Background())
	d := &Discover{
		client:        nil,
		locker:        &sync.RWMutex{},
		cache:         make(map[string]SvcCache),
		validDuration: 10 * time.Second,
		// sf:            &singleflight.Group{},
		ctx:      ctx,
		cancel:   cancel,
		watchSvc: make(map[string]struct{}),
		wg:       &sync.WaitGroup{},
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endPoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	d.client = client

	return d, nil
}

type Discover struct {
	client        *clientv3.Client
	locker        *sync.RWMutex
	cache         map[string]SvcCache
	validDuration time.Duration
	// sf            *singleflight.Group
	ctx      context.Context
	cancel   context.CancelFunc
	watchSvc map[string]struct{}
	wg       *sync.WaitGroup
}

type SvcCache struct {
	UpdateTime time.Time
	Items      map[string]ServiceItem
}

func (d SvcCache) Addr() []string {
	ret := make([]string, 0, len(d.Items))
	for _, item := range d.Items {
		ret = append(ret, item.Addr)
	}
	return ret
}

func (d *Discover) Watch(svcName string) {
	d.watchSvc[svcName] = struct{}{}
}

func (d *Discover) sync(svcName string) (err error) {
	resp, err := d.client.Get(d.ctx, servicePrefix(svcName), clientv3.WithPrefix())
	if err != nil {
		return err
	}

	discoverItemSet := make(map[string]ServiceItem)
	for _, kv := range resp.Kvs {
		var svcItem ServiceItem
		err := json.Unmarshal(kv.Value, &svcItem)
		if err != nil {
			log.Printf("discover unmarshal service error: %s-%v\n", svcName, err.Error())
			continue
		}

		discoverItemSet[svcItem.Addr] = svcItem
	}
	c := SvcCache{
		UpdateTime: time.Now(),
		Items:      discoverItemSet,
	}
	d.locker.Lock()
	d.cache[svcName] = c
	d.locker.Unlock()
	return nil
}

func (d *Discover) watch(svcName string) {
	watchChan := d.client.Watch(d.ctx, servicePrefix(svcName), clientv3.WithPrefix())
	go func() {
		defer d.wg.Done()

		for result := range watchChan {
			_ = result
			err := d.sync(svcName)
			if err != nil {
				log.Printf("discover watch sync error: %s-%v\n", svcName, err.Error())
			}
		}

		log.Printf("discover watch chan closed: %s\n", svcName)
		// TODO 若未关闭，则重连 go d.Watch(svcName)
	}()
}

func (d *Discover) queryCache(svcName string) ([]string, bool) {
	d.locker.RLock()
	defer d.locker.RUnlock()
	cacheItem, ok := d.cache[svcName]
	if !ok {
		return nil, false
	}
	return cacheItem.Addr(), true
}

func (d *Discover) QueryService(svcName string) []string {
	addr, ok := d.queryCache(svcName)
	if ok {
		return addr
	}

	err := d.sync(svcName)
	if err != nil {
		log.Printf("discover sync error: %s-%v\n", svcName, err.Error())
		return nil
	}
	addr, _ = d.queryCache(svcName)
	return addr
}

func (d *Discover) Start() {
	d.wg.Add(len(d.watchSvc))
	for svcName := range d.watchSvc {
		d.watch(svcName)
	}
}

func (d *Discover) Stop() {
	d.cancel()
	d.wg.Wait()
}
