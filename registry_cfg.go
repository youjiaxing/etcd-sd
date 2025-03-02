package sd

import (
	"strings"
	"time"
)

type RegistryCfg struct {
	protect bool

	namespace    string        // 命名空间, 以 / 开头和结尾
	ttl          int64         // lease ttl
	retryTimeout time.Duration // 重试超时时间
}

func (c *RegistryCfg) WithRetryTimeout(timeout time.Duration) *RegistryCfg {
	if timeout <= 0 {
		return c
	}
	c.retryTimeout = timeout
	return c
}

func (c *RegistryCfg) WithTTL(ttl int64) *RegistryCfg {
	if ttl <= 0 {
		return c
	}
	c.ttl = ttl
	return c
}

func (c *RegistryCfg) WithNamespace(namespace string) *RegistryCfg {
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

func NewRegistryCfg() *RegistryCfg {
	return &RegistryCfg{
		protect:      true,
		namespace:    "/service/",
		ttl:          5,
		retryTimeout: time.Second * 5,
	}
}
