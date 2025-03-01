package sd_test

import (
	"context"
	"github.com/youjiaxing/sd"
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

	if err := registry.Reg(ctx, svc); err != nil {
		log.Fatalf("register service failed: %v", err)
	}

	// 模拟服务运行
	time.Sleep(30 * time.Second)

	// 注销服务
	log.Println("unregister service")
	if err := registry.UnReg(ctx, svc.Name, svc.Id); err != nil {
		log.Printf("unregister service failed: %v", err)
	}

	// 停止注册中心
	log.Println("stop registry")
	registry.Stop()

	// Output:
}
