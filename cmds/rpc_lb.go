//
//  Paranoid Pirate queue. 参考: http://zguide.zeromq.org/php:chapter4
//
package main

import (
	rpc_commons "git.chunyu.me/infra/rpc_commons"
	lb "git.chunyu.me/infra/rpc_proxy/lb"
	utils "git.chunyu.me/infra/rpc_proxy/utils"
)

const (
	BINARY_NAME  = "rpc_lb"
	SERVICE_DESC = "Chunyu RPC Load Balance Service"
)

func main() {
	rpc_commons.RpcMain(BINARY_NAME, SERVICE_DESC,
		// 验证LB的配置
		rpc_commons.ConfigCheckRpcLB,
		// 根据配置创建一个Server
		func(config *utils.Config) rpc_commons.Server {
			return lb.NewLoadBalanceServer(config)
		})

}
