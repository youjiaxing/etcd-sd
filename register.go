package sd

import (
	"context"
	"encoding/json"
	"fmt"
	"go.etcd.io/etcd/client/v3"
	"log"
	"time"
)

func NewRegister(endPoints []string) (*Register, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endPoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	return &Register{
		ctx:         context.Background(),
		client:      client,
		storage:     make([]serviceItem, 0),
		grantId:     0,
		doneCh:      make(chan struct{}),
		grantCancel: nil,
	}, nil
}

type Register struct {
	ctx         context.Context
	client      *clientv3.Client
	storage     []serviceItem
	grantId     clientv3.LeaseID
	doneCh      chan struct{}
	grantCancel func()
}

type serviceItem struct {
	SvcName string `json:"svc_name"`
	Addr    string `json:"addr"`
}

func (s serviceItem) String() string {
	bytes, _ := json.Marshal(s)
	return string(bytes)
}

func (r *Register) checkGrant() error {
	if r.grantId != 0 {
		return nil
	}

	grantCtx, cancel := context.WithCancel(r.ctx)
	grantResp, err := r.client.Grant(grantCtx, 5)
	if err != nil {
		defer cancel()
		return err
	}
	r.grantCancel = cancel

	ch, err := r.client.KeepAlive(grantCtx, grantResp.ID)
	if err != nil {
		return err
	}
	r.grantId = grantResp.ID

	go func() {
		defer func() {
			close(r.doneCh)
		}()
		for ka := range ch {
			kaStr, _ := json.Marshal(ka)
			log.Printf("keep alive resp: %s\n", kaStr)
		}
	}()
	return nil
}

func (r *Register) Reg(svcName string, addr string) {
	item := serviceItem{
		SvcName: svcName,
		Addr:    addr,
	}
	r.storage = append(r.storage, item)
}

func (r *Register) Start() error {
	err := r.checkGrant()
	if err != nil {
		return err
	}

	for _, item := range r.storage {
		_, err = r.client.Put(r.ctx, fmt.Sprintf("/service/%s/%s", item.SvcName, item.Addr), item.String(), clientv3.WithLease(r.grantId))
		if err != nil {
			r.grantCancel()
			return err
		}
	}
	return err
}

func (r *Register) Stop() {
	if r.grantCancel == nil {
		return
	}
	r.grantCancel()
	_, err := r.client.Revoke(r.ctx, r.grantId)
	if err != nil {
		log.Printf("Register stop error: %v\n", err)
	}
	<-r.doneCh
}
