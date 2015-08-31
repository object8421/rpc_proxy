//
//  Paranoid Pirate queue. 参考: http://zguide.zeromq.org/php:chapter4
//
package main

import (
	proxy "git.chunyu.me/infra/rpc_proxy/proxy"
	utils "git.chunyu.me/infra/rpc_proxy/utils"
)

const (
	BINARY_NAME  = "rpc_lb"
	SERVICE_DESC = "Chunyu RPC Load Balance Service"
)

func main() {
	proxy.RpcMain(BINARY_NAME, SERVICE_DESC,
		// 验证LB的配置
		proxy.ConfigCheckRpcLB,
		// 根据配置创建一个Server
		func(config *utils.Config) proxy.Server {
			return proxy.NewThriftLoadBalanceServer(config)
		})

}
