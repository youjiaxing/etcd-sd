package etcdsd_test

import (
	"context"
	"fmt"
	sd "github.com/youjiaxing/etcd-sd"
	"go.etcd.io/etcd/client/v3"
	"log"
	"time"
)

func ExampleRegistry() {
	// 初始化etcd客户端
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("connect etcd failed: %v", err)
	}
	defer cli.Close()

	// 创建注册中心配置
	cfg := sd.NewRegistryCfg()

	// 创建注册中心实例
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	registry, err := sd.NewRegistry(ctx, cli, cfg)
	if err != nil {
		log.Fatalf("create registry failed: %v", err)
	}

	// 注册示例服务
	svc := sd.Service{
		Name: "user-service",
		Id:   "instance-1",
		Addr: []string{"127.0.0.1:8080"},
	}

	if err := registry.Reg(svc); err != nil {
		log.Fatalf("register service failed: %v", err)
	}

	// 模拟服务运行
	time.Sleep(5 * time.Second)

	// 注销服务
	log.Println("unregister service")
	if err := registry.UnReg(svc.Name, svc.Id); err != nil {
		log.Printf("unregister service failed: %v", err)
	}

	// 停止注册中心
	log.Println("stop registry")
	registry.Stop()

	// Output:
}

// 完整场景测试：服务注册 -> 服务发现 -> 变更通知 -> 服务注销
func Example_fullFlow() {
	// 初始化etcd客户端
	cli := setupEtcdClientForExample()
	defer cli.Close()

	// ========== 服务注册侧 ==========
	registryCfg := sd.NewRegistryCfg().
		WithNamespace("/prod/services/").
		WithTTL(10) // 10秒TTL

	// 创建注册中心
	regCtx, regCancel := context.WithCancel(context.Background())
	defer regCancel()

	registry, err := sd.NewRegistry(regCtx, cli, registryCfg)
	if err != nil {
		log.Fatalf("创建注册中心失败: %v", err)
	}
	defer registry.Stop()

	// 注册两个支付服务实例
	paymentService := []sd.Service{
		{Name: "payment", Id: "node-1", Addr: []string{"10.0.0.1:8080"}},
		{Name: "payment", Id: "node-2", Addr: []string{"10.0.0.2:8080"}},
	}

	for _, svc := range paymentService {
		if err := registry.Reg(svc); err != nil {
			log.Fatalf("注册服务失败: %s", svc.Id)
		}
	}

	// ========== 服务发现侧 ==========
	discoverCfg := sd.NewDiscoverCfg().
		WithNamespace("/prod/services/").
		WithRetryTimeout(time.Second)

	// 创建发现客户端
	discover, err := sd.NewDiscover(context.Background(), cli, discoverCfg)
	if err != nil {
		log.Fatalf("创建发现客户端失败: %v", err)
	}

	// 查询服务实例
	services, err := discover.Query(context.Background(), "payment")
	if err != nil {
		log.Fatalf("服务查询失败: %v", err)
	}
	fmt.Printf("初始发现服务数: %d\n", len(services))

	// 启动监听
	watcher, err := discover.Watch("payment")
	if err != nil {
		log.Fatalf("启动监听失败: %v", err)
	}
	defer watcher.Close()

	// 启动协程监听变更通知
	go func() {
		for range watcher.Notify() {
			services := watcher.Get()
			fmt.Printf("收到变更通知，当前服务数: %d\n", len(services))
		}
	}()

	// ========== 模拟动态扩缩容 ==========
	time.Sleep(1 * time.Second)

	// 扩容：新增实例
	newSvc := sd.Service{
		Name: "payment",
		Id:   "node-3",
		Addr: []string{"10.0.0.3:8080"},
	}
	if err := registry.Reg(newSvc); err != nil {
		log.Printf("扩容失败: %v", err)
	}

	// 缩容：移除实例
	time.Sleep(1 * time.Second)
	if err := registry.UnReg("payment", "node-1"); err != nil {
		log.Printf("缩容失败: %v", err)
	}

	// ========== 清理阶段 ==========
	time.Sleep(time.Second)
	registry.Stop()
	time.Sleep(1 * time.Second) // 等待资源释放

	// 最终查询
	services, _ = discover.Query(context.Background(), "payment")
	fmt.Printf("最终服务数: %d", len(services))

	// Output:
	// 初始发现服务数: 2
	// 收到变更通知，当前服务数: 3
	// 收到变更通知，当前服务数: 2
	// 收到变更通知，当前服务数: 1
	// 收到变更通知，当前服务数: 0
	// 最终服务数: 0
}

// 初始化etcd客户端
func setupEtcdClientForExample() *clientv3.Client {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 3 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}

	// 清理测试数据
	defer cli.Delete(context.Background(), "/prod/", clientv3.WithPrefix())

	return cli
}
