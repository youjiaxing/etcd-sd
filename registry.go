package etcdsd

import (
	"context"
	"encoding/json"
	"fmt"
	"go.etcd.io/etcd/client/v3"
	"log"
	"sync"
	"time"
)

// IRegistry 服务注册
type IRegistry interface {
	Reg(svc Service) error
	UnReg(name, id string) error
	Stop()
}

var _ IRegistry = (*Registry)(nil)

type Service struct {
	Name     string            `json:"name"`               // 服务名
	Id       string            `json:"id"`                 // 服务提供方id
	Metadata map[string]string `json:"metadata,omitempty"` // 服务元数据
	Addr     []string          `json:"endpoint"`           // 服务地址
}

func (s *Service) Valid() bool {
	if s.Name == "" || s.Id == "" || len(s.Addr) == 0 {
		return false
	}
	return true
}

func (s *Service) Key() string {
	return RegSvcKey(s.Name, s.Id)
}

func (s *Service) Marshal() string {
	bytes, _ := json.Marshal(s)
	return string(bytes)
}

func (s *Service) Unmarshal(raw string) error {
	return json.Unmarshal([]byte(raw), s)
}

func NewRegistry(ctx context.Context, client *clientv3.Client, cfg *RegistryCfg) (*Registry, error) {
	if !cfg.protect {
		return nil, fmt.Errorf("registr cfg must init by NewRegistryCfg()")
	}

	ctx, ctxCancel := context.WithCancel(ctx)
	return &Registry{
		ctx:        ctx,
		ctxCancel:  ctxCancel,
		client:     client,
		cfg:        *cfg,
		stopWg:     &sync.WaitGroup{},
		regMutex:   &sync.RWMutex{},
		regService: make(map[string]*regService),
	}, nil
}

// 注册的服务
type regService struct {
	svc       Service
	ctx       context.Context
	ctxCancel func()
	leaseId   clientv3.LeaseID
	kaChan    <-chan *clientv3.LeaseKeepAliveResponse
}

type Registry struct {
	ctx       context.Context
	ctxCancel func()
	client    *clientv3.Client
	cfg       RegistryCfg
	stopWg    *sync.WaitGroup

	regMutex   *sync.RWMutex          // protect below
	regService map[string]*regService // key: Service.Key()
}

func (r *Registry) UnReg(name, id string) error {
	r.regMutex.Lock()
	defer r.regMutex.Unlock()
	svcKey := RegSvcKey(name, id)
	regSvc, has := r.regService[svcKey]
	if !has {
		return fmt.Errorf("service %s not found", name)
	}
	regSvc.ctxCancel()
	delete(r.regService, svcKey)
	return nil
}

func (r *Registry) Reg(svc Service) error {
	if !svc.Valid() {
		return fmt.Errorf("invalid service")
	}
	// 检测是否已经注册过
	_, duplicate := r.getRegSvc(svc.Key())
	if duplicate {
		return fmt.Errorf("duplicate service %s", svc.Key())
	}

	ctx, cancel := context.WithCancel(r.ctx)
	leaseId, kaChan, err := r.putWithNewLease(ctx, svc)
	if err != nil {
		cancel()
		return err
	}

	r.stopWg.Add(1)
	r.setRegSvc(regService{
		svc:       svc,
		ctx:       ctx,
		ctxCancel: cancel,
		leaseId:   leaseId,
		kaChan:    kaChan,
	})
	go r.keepAliveLease(ctx, svc.Key())
	return nil
}

func (r *Registry) setRegSvc(regSvc regService) {
	r.regMutex.Lock()
	defer r.regMutex.Unlock()
	r.regService[regSvc.svc.Key()] = &regSvc
}

func (r *Registry) getRegSvc(svcKey string) (*regService, bool) {
	r.regMutex.RLock()
	defer r.regMutex.RUnlock()
	regSvc, ok := r.regService[svcKey]
	return regSvc, ok
}

func (r *Registry) keepAliveLease(ctx context.Context, svcKey string) {
	regSvc, ok := r.getRegSvc(svcKey)
	if !ok {
		return
	}
	defer func() {
		_, _ = r.client.Lease.Revoke(context.Background(), regSvc.leaseId)
		r.stopWg.Done()
		return
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-regSvc.kaChan:
			if ok {
				continue
			}
			log.Printf("keepAliveLease error: %s kaChan close\n", svcKey)

			if ctx.Err() != nil {
				return
			}
			time.Sleep(r.cfg.retryTimeout)
			if ctx.Err() != nil {
				return
			}

			// 重新创建
			leaseId, kaChan, err := r.putWithNewLease(ctx, regSvc.svc)
			if err != nil {
				log.Printf("keepAliveLease recreate error: %s %s\n", svcKey, err.Error())
				continue
			}
			regSvc.kaChan = kaChan
			regSvc.leaseId = leaseId
		}
	}
}

func (r *Registry) putWithNewLease(ctx context.Context, svc Service) (leaseId clientv3.LeaseID, kaChan <-chan *clientv3.LeaseKeepAliveResponse, err error) {
	leaseResp, err := r.client.Lease.Grant(ctx, r.cfg.ttl)
	if err != nil {
		return 0, nil, err
	}
	leaseId = leaseResp.ID

	kaChan, err = r.client.Lease.KeepAlive(ctx, leaseId)
	if err != nil {
		return 0, nil, err
	}
	defer func() {
		if err != nil {
			_, revokeErr := r.client.Lease.Revoke(context.Background(), leaseId)
			if revokeErr != nil {
				log.Printf("revoke lease error: %s %s\n", svc.Key(), revokeErr.Error())
			}
		}
	}()

	key := r.cfg.ServiceInstEtcdKey(svc.Name, svc.Id)
	value := svc.Marshal()
	_, err = r.client.KV.Put(ctx, key, value, clientv3.WithLease(leaseResp.ID))
	if err != nil {
		return 0, nil, err
	}

	return leaseId, kaChan, nil
}

func (r *Registry) Stop() {
	r.ctxCancel()
	r.regMutex.Lock()
	r.regService = make(map[string]*regService)
	r.regMutex.Unlock()

	r.stopWg.Wait()
}
