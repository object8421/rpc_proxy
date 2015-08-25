package main

import (
	rpc_commons "git.chunyu.me/infra/rpc_commons"
	proxy "git.chunyu.me/infra/rpc_proxy/proxy"
	utils "git.chunyu.me/infra/rpc_proxy/utils"
)

const (
	BINARY_NAME  = "rpc_proxy"
	SERVICE_DESC = "Chunyu RPC Local Proxy v0.1"
)

func main() {
	rpc_commons.RpcMain(BINARY_NAME, SERVICE_DESC,
		// 验证LB的配置
		rpc_commons.ConfigCheckRpcProxy,
		// 根据配置创建一个Server
		func(config *utils.Config) rpc_commons.Server {
			// 正式的服务
			return proxy.NewProxyServer(config)
		})

}
