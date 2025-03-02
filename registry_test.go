package sd_test

import (
	"context"
	"fmt"
	"github.com/youjiaxing/sd"
	"go.etcd.io/etcd/client/v3"
	"runtime/debug"
	"strings"
	"sync"
	"testing"
	"time"
)

// 测试工具函数
func setupRegistry(t *testing.T, cfg *sd.RegistryCfg) (*sd.Registry, *clientv3.Client) {
	t.Helper()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("连接etcd失败: %v", err)
	}

	// ctx, cancel := context.WithCancel(context.Background())
	// t.Cleanup(cancel)

	ctx := context.Background()
	registry, err := sd.NewRegistry(ctx, cli, cfg)
	if err != nil {
		t.Fatalf("创建注册中心失败: %v", err)
	}
	// t.Cleanup(registry.Stop)

	return registry, cli
}

func TestMultipleInstances(t *testing.T) {
	ttl := int64(5)
	registry, cli := setupRegistry(t, sd.NewRegistryCfg().WithTTL(ttl))

	// 准备3个服务实例
	services := []sd.Service{
		{Name: "payment", Id: "instance-1", Addr: []string{"10.0.0.1:8080"}},
		{Name: "payment", Id: "instance-2", Addr: []string{"10.0.0.2:8080"}},
		{Name: "payment", Id: "instance-3", Addr: []string{"10.0.0.3:8080"}},
	}

	// 并发注册测试
	var wg sync.WaitGroup
	for _, svc := range services {
		wg.Add(1)
		go func(s sd.Service) {
			defer wg.Done()
			if err := registry.Reg(s); err != nil {
				t.Errorf("注册服务 %s 失败: %v", s.Id, err)
			}
		}(svc)
	}
	wg.Wait()

	// 验证etcd中的键数量
	prefixKey := "/service/payment/"
	_, _ = validateEtcdCount(t, cli, prefixKey, len(services))

	// 注销一个实例测试
	time.Sleep(time.Second * 2)
	delSvc := services[0]
	services = services[1:]
	if err := registry.UnReg(delSvc.Name, delSvc.Id); err != nil {
		t.Errorf("注销失败: %v", err)
	}

	// 再次验证剩余实例
	time.Sleep(time.Second)
	resp, _ := validateEtcdCount(t, cli, prefixKey, len(services))
	for _, kv := range resp.Kvs {
		if strings.Contains(string(kv.Key), delSvc.Key()) {
			t.Error("发现未正确删除的实例键")
		}
	}

	// 等待1个 ttl 后再次验证 etcd 中的数量
	time.Sleep(time.Duration(ttl) * time.Second)
	_, _ = validateEtcdCount(t, cli, prefixKey, len(services))

	registry.Stop()

	// 再次验证 etcd 中的数量
	_, _ = validateEtcdCount(t, cli, prefixKey, 0)
}

// 将验证 etcd 中数量提取为一个函数
func validateEtcdCount(t *testing.T, cli *clientv3.Client, prefixKey string, expectedCount int) (*clientv3.GetResponse, error) {
	resp, err := cli.Get(context.Background(), prefixKey, clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("查询etcd失败: %v", err)
	}
	if len(resp.Kvs) != expectedCount {
		t.Errorf("预期%d个实例，实际发现 %d 个，堆栈:\n%s", expectedCount, len(resp.Kvs), string(debug.Stack()))
	}
	return resp, err
}

func TestLeaseRenewal(t *testing.T) {
	cfg := sd.NewRegistryCfg().WithNamespace("/TestLease").WithTTL(3)
	registry, cli := setupRegistry(t, cfg)
	ctx := context.Background()

	// 注册测试服务
	svc := sd.Service{
		Name: "inventory",
		Id:   "node-1",
		Addr: []string{"10.0.1.10:8080"},
	}
	if err := registry.Reg(svc); err != nil {
		t.Fatalf("注册失败: %v", err)
	}
	svcKey := cfg.ServiceInstEtcdKey("inventory", "node-1")

	// 获取初始租约信息
	initialResp, err := cli.Get(ctx, svcKey)
	if err != nil || len(initialResp.Kvs) == 0 {
		t.Fatal("服务未正确注册")
	}

	// 等待超过TTL时间（3秒）后验证续期
	time.Sleep(5 * time.Second)

	// 检查服务仍然存在
	resp, err := cli.Get(ctx, svcKey)
	if err != nil || len(resp.Kvs) == 0 {
		t.Error("租约未正确续期")
	}

	// 停止注册
	registry.Stop()

	// 等待TTL过期后验证自动删除
	time.Sleep(time.Second)
	resp, _ = cli.Get(ctx, svcKey)
	if len(resp.Kvs) > 0 {
		t.Error("服务未自动过期")
	}
}

// 并发压力测试
func TestConcurrentRegistration(t *testing.T) {
	registry, cli := setupRegistry(t, sd.NewRegistryCfg().WithNamespace("/test/services/"))
	ctx := context.Background()
	const N = 500

	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(id int) {
			defer wg.Done()
			svc := sd.Service{
				Name: "search",
				Id:   fmt.Sprintf("worker-%d", id),
				Addr: []string{fmt.Sprintf("10.0.2.%d:8080", id)},
			}
			if err := registry.Reg(svc); err != nil {
				t.Errorf("注册失败: worker-%d: %v", id, err)
			}
		}(i)
	}
	wg.Wait()

	// 验证注册数量
	resp, err := cli.Get(ctx, "/test/services/search/", clientv3.WithPrefix())
	if err != nil || len(resp.Kvs) != N {
		t.Errorf("并发注册异常，预期50实例，实际 %d", len(resp.Kvs))
	}

	registry.Stop()
}
