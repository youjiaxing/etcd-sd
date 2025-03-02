package etcdsd

import (
	"strings"
	"time"
)

type DiscoverCfg struct {
	protect bool

	namespace    string        // 命名空间, 以 / 开头和结尾
	retryTimeout time.Duration // 重试超时时间
}

func (c *DiscoverCfg) WithRetryTimeout(timeout time.Duration) *DiscoverCfg {
	if timeout <= 0 {
		return c
	}
	c.retryTimeout = timeout
	return c
}

func (c *DiscoverCfg) WithNamespace(namespace string) *DiscoverCfg {
	namespace = strings.ReplaceAll(namespace, "//", "/")
	if !strings.HasPrefix(namespace, "/") {
		namespace = "/" + namespace
	}
	if !strings.HasSuffix(namespace, "/") {
		namespace = namespace + "/"
	}
	c.namespace = namespace
	return c
}

func (c *DiscoverCfg) ServiceInstEtcdPrefixKey(svcName string) string {
	return ServiceInstEtcdPrefixKey(c.namespace, svcName)
}

func (c *DiscoverCfg) ServiceInstEtcdKey(svcName string, svcId string) string {
	return ServiceInstEtcdKey(c.namespace, svcName, svcId)
}

func NewDiscoverCfg() *DiscoverCfg {
	return &DiscoverCfg{
		protect:      true,
		namespace:    DefaultNameSpace,
		retryTimeout: time.Second * 5,
	}
}
