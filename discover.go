package etcdsd

import (
	"context"
	"fmt"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log"
	"sync"
	"time"
)

type IDiscover interface {
	Query(ctx context.Context, name string) ([]Service, error)
	Watch(svcName string) (IWatcher, error)
}

type IWatcher interface {
	Get() []Service
	Notify() <-chan struct{}
	Close()
}

var _ IDiscover = (*Discover)(nil)

func NewDiscover(
	ctx context.Context,
	client *clientv3.Client,
	cfg *DiscoverCfg,
) (*Discover, error) {
	if !cfg.protect {
		return nil, fmt.Errorf("discover cfg must init by NewDiscoverCfg()")
	}

	ctx, cancel := context.WithCancel(ctx)
	return &Discover{
		DiscoverCfg: *cfg,
		client:      client,
		ctx:         ctx,
		ctxCancel:   cancel,
		watcher:     new(sync.Map),
		wg:          new(sync.WaitGroup),
	}, nil
}

type Discover struct {
	DiscoverCfg
	client    *clientv3.Client
	ctx       context.Context
	ctxCancel context.CancelFunc
	watcher   *sync.Map // map[string]*Watcher
	wg        *sync.WaitGroup
}

func (d *Discover) Watch(svcName string) (IWatcher, error) {
	v, ok := d.watcher.Load(svcName)
	if ok {
		return v.(*Watcher), nil
	}

	getResp, svcList, err := d.query(d.ctx, svcName)
	if err != nil {
		return nil, err
	}

	d.wg.Add(1)
	w := NewWatcher(d.ctx, d.client, d.DiscoverCfg, d.wg, svcName, svcList)
	go w.Loop(getResp.Header.Revision + 1)
	d.watcher.Store(svcName, w)
	return w, nil
}

func (d *Discover) Query(ctx context.Context, svcName string) ([]Service, error) {
	_, svcList, err := d.query(ctx, svcName)
	return svcList, err
}

func (d *Discover) query(ctx context.Context, svcName string) (*clientv3.GetResponse, []Service, error) {
	return querySvcList(ctx, d.client, d.ServiceInstEtcdPrefixKey(svcName))
}

func querySvcList(ctx context.Context, client *clientv3.Client, prefixKey string) (*clientv3.GetResponse, []Service, error) {
	getResp, err := client.Get(ctx, prefixKey, clientv3.WithPrefix())
	if err != nil {
		return nil, nil, err
	}

	var services []Service
	for _, kv := range getResp.Kvs {
		var svc Service
		err = svc.Unmarshal(string(kv.Value))
		if err != nil {
			log.Printf("querySvcList unmarshal service error: %s-%v. raw value: %s\n", prefixKey, err.Error(), kv.Value)
			continue
		}
		services = append(services, svc)
	}
	return getResp, services, nil
}

func NewWatcher(ctx context.Context, client *clientv3.Client, cfg DiscoverCfg, wg *sync.WaitGroup, svcName string, initSvcList []Service) *Watcher {
	ctx, cancel := context.WithCancel(ctx)
	svcCache := make(map[string]Service)
	for _, svc := range initSvcList {
		svcCache[cfg.ServiceInstEtcdKey(svc.Name, svc.Id)] = svc
	}

	return &Watcher{
		DiscoverCfg: cfg,
		svcName:     svcName,
		client:      client,
		ctx:         ctx,
		ctxCancel:   cancel,
		wg:          wg,
		notify:      make(chan struct{}, 1),
		cacheLocker: new(sync.RWMutex),
		svcCache:    svcCache,
	}
}

type Watcher struct {
	DiscoverCfg
	svcName   string
	client    *clientv3.Client
	ctx       context.Context
	ctxCancel context.CancelFunc
	notify    chan struct{}
	wg        *sync.WaitGroup

	cacheLocker *sync.RWMutex
	svcCache    map[string]Service // key: etcd key
}

func (w *Watcher) Loop(startRev int64) {
	defer func() {
		close(w.notify)
		w.wg.Done()
	}()

startLoop:
	watchChan := w.client.Watch(w.ctx, w.ServiceInstEtcdPrefixKey(w.svcName), clientv3.WithRev(startRev), clientv3.WithPrefix())
	for {
		select {
		case <-w.ctx.Done():
			return
		case watchResp, ok := <-watchChan:
			if !ok {
				// watch closed
				return
			}
			if watchResp.Err() != nil {
				// watch failed
				log.Printf("Watcher watch error: %s-%v\n", w.svcName, watchResp.Err().Error())
				if w.ctx.Err() != nil {
					return
				}
				time.Sleep(w.retryTimeout)
				if w.ctx.Err() != nil {
					return
				}
				goto startLoop
			}

			for _, e := range watchResp.Events {
				w.cacheLocker.Lock()
				switch e.Type {
				case mvccpb.DELETE:
					delete(w.svcCache, string(e.Kv.Key))
				case mvccpb.PUT:
					var svc Service
					err := svc.Unmarshal(string(e.Kv.Value))
					if err != nil {
						log.Printf("Watcher unmarshal service error: %s-%v. raw value: %s\n", w.svcName, err.Error(), e.Kv.Value)
						continue
					}
					w.svcCache[string(e.Kv.Key)] = svc
				}
				w.cacheLocker.Unlock()
			}

			select {
			case w.notify <- struct{}{}:
			default:
			}
		}
	}
}

func (w *Watcher) Get() []Service {
	w.cacheLocker.RLock()
	defer w.cacheLocker.RUnlock()
	svcList := make([]Service, 0, len(w.svcCache))
	for _, svc := range w.svcCache {
		svcList = append(svcList, svc)
	}
	return svcList
}

func (w *Watcher) Notify() <-chan struct{} {
	return w.notify
}

func (w *Watcher) Close() {
	w.ctxCancel()
}
