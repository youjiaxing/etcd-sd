package etcdsd_test

import (
	"context"
	"fmt"
	sd "github.com/youjiaxing/etcd-sd"
	"testing"
	"time"

	"go.etcd.io/etcd/client/v3"
)

func TestDiscover_Query(t *testing.T) {
	// 初始化测试etcd客户端
	cli := setupEtcdClient(t)

	// 准备测试数据
	svc1 := sd.Service{Name: "payment", Id: "node-1", Addr: []string{"10.0.0.1:8080"}}
	svc2 := sd.Service{Name: "payment", Id: "node-2", Addr: []string{"10.0.0.2:8080"}}
	putServices(t, cli, "payment", svc1, svc2)

	// 创建Discover实例
	discover := newDiscover(t, cli)

	t.Run("查询存在的服务", func(t *testing.T) {
		services, err := discover.Query(context.Background(), "payment")
		if err != nil {
			t.Fatalf("查询失败: %v", err)
		}
		if len(services) != 2 {
			t.Errorf("期望2个实例，实际获取%d个", len(services))
		}
	})

	t.Run("查询不存在的服务", func(t *testing.T) {
		services, err := discover.Query(context.Background(), "inventory")
		if err != nil {
			t.Fatalf("查询失败: %v", err)
		}
		if len(services) != 0 {
			t.Errorf("期望0个实例，实际获取%d个", len(services))
		}
	})
}

func TestDiscover_Watch(t *testing.T) {
	cli := setupEtcdClient(t)

	discover := newDiscover(t, cli)

	t.Run("基础监听测试", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		// 初始注册服务
		svc := sd.Service{Name: "search", Id: "worker-1", Addr: []string{"10.0.1.1:8080"}}
		putServices(t, cli, "search", svc)

		// 启动监听
		watcher, err := discover.Watch("search")
		if err != nil {
			t.Fatalf("启动监听失败: %v", err)
		}
		defer watcher.Close()

		// 验证初始数据
		if len(watcher.Get()) != 1 {
			t.Fatal("初始服务数量不正确")
		}

		// 添加新服务
		newSvc := sd.Service{Name: "search", Id: "worker-2", Addr: []string{"10.0.1.2:8080"}}
		putServices(t, cli, "search", newSvc)

		// 等待变更通知
		select {
		case <-watcher.Notify():
			if len(watcher.Get()) != 2 {
				t.Error("未检测到新增服务")
			}
		case <-ctx.Done():
			t.Fatal("等待变更通知超时")
		}

		// 删除服务
		deleteServices(t, cli, "search", "worker-1")

		// 再次验证
		select {
		case <-watcher.Notify():
			services := watcher.Get()
			if len(services) != 1 || services[0].Id != "worker-2" {
				t.Error("未正确处理删除操作")
			}
		case <-ctx.Done():
			t.Fatal("等待删除通知超时")
		}
	})
}

// 测试工具函数
func setupEtcdClient(t *testing.T) *clientv3.Client {
	backoffWaitBetween := 100 * time.Millisecond
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:          []string{"localhost:2379"},
		DialTimeout:        2 * time.Second,
		BackoffWaitBetween: backoffWaitBetween,
	})
	if err != nil {
		t.Fatalf("连接etcd失败: %v", err)
	}
	t.Cleanup(func() {
		// 清理测试数据
		if _, err := cli.Delete(context.Background(), "/test/", clientv3.WithPrefix()); err != nil {
			t.Logf("清理etcd数据失败: %v", err)
		}
	})
	return cli
}

func newDiscover(t *testing.T, cli *clientv3.Client) *sd.Discover {
	d, err := sd.NewDiscover(cli.Ctx(), cli, sd.NewDiscoverCfg().WithNamespace("/test/services/").WithRetryTimeout(time.Second))
	if err != nil {
		t.Fatalf("创建Discover失败: %v", err)
	}
	return d
}

func putServices(t *testing.T, cli *clientv3.Client, svcName string, services ...sd.Service) {
	for _, svc := range services {
		key := fmt.Sprintf("/test/services/%s/%s", svcName, svc.Id)
		if _, err := cli.Put(context.Background(), key, svc.Marshal()); err != nil {
			t.Fatalf("准备测试数据失败: %v", err)
		}
	}
}

func deleteServices(t *testing.T, cli *clientv3.Client, svcName string, ids ...string) {
	for _, id := range ids {
		key := fmt.Sprintf("/test/services/%s/%s", svcName, id)
		if _, err := cli.Delete(context.Background(), key); err != nil {
			t.Fatalf("删除测试数据失败: %v", err)
		}
	}
}

func TestDiscover_ErrorScenarios(t *testing.T) {
	t.Run("数据解析失败", func(t *testing.T) {
		cli := setupEtcdClient(t)
		key := "/test/services/logging/bad-data"
		if _, err := cli.Put(context.Background(), key, "invalid-json"); err != nil {
			t.Fatal(err)
		}

		discover := newDiscover(t, cli)
		services, err := discover.Query(context.Background(), "logging")
		if err != nil {
			t.Fatal(err)
		}
		if len(services) != 0 {
			t.Error("预期过滤无效数据")
		}
	})
}
