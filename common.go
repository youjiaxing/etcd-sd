package etcdsd

import "fmt"

func servicePrefix(name string) string {
	return fmt.Sprintf("/service/%s/", name)
}
