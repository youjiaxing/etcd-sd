package etcdsd

import "fmt"

const (
	DefaultNameSpace = "/service/"
)

// ServiceInstEtcdKey 服务实例 Key
func ServiceInstEtcdKey(namespace string, svcName string, svcId string) string {
	return fmt.Sprintf("%s%s/%s", namespace, svcName, svcId)
}

// ServiceInstEtcdPrefixKey 服务实例前缀 Key
func ServiceInstEtcdPrefixKey(namespace string, svcName string) string {
	return fmt.Sprintf("%s%s/", namespace, svcName)
}

func RegSvcKey(svcName, svcId string) string {
	return fmt.Sprintf("%s-%s", svcName, svcId)
}
