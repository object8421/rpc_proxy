package proxy

import (
	utils "git.chunyu.me/infra/rpc_proxy/utils"
	"git.chunyu.me/infra/rpc_proxy/utils/log"
)

type ConfigCheck func(conf *utils.Config)

//
// 一般的ThriftService的配置检测
//
func ConfigCheckThriftService(conf *utils.Config) {
	if conf.ProductName == "" {
		log.Panic("Invalid ProductName")
	}
	if conf.FrontendAddr == "" {
		log.Panic("Invalid FrontendAddress")
	}

	if conf.Service == "" {
		log.Panic("Invalid ServiceName")
	}

	if conf.ZkAddr == "" {
		log.Panic("Invalid zookeeper address")
	}
}

//
// RPC Proxy的Config Checker
//
func ConfigCheckRpcProxy(conf *utils.Config) {
	if conf.ProductName == "" {
		log.Panic("Invalid ProductName")
	}
	if conf.ZkAddr == "" {
		log.Panic("Invalid zookeeper address")
	}
	if conf.ProxyAddr == "" {
		log.Panic("Invalid Proxy address")
	}
}

//
// RPC LB的Config Checker
//
func ConfigCheckRpcLB(conf *utils.Config) {
	if conf.ProductName == "" {
		log.Panic("Invalid ProductName")
	}

	if conf.ZkAddr == "" {
		log.Panic("Invalid zookeeper address")
	}

	if conf.Service == "" {
		log.Panic("Invalid ServiceName")
	}

	if conf.BackAddr == "" {
		log.Panic("Invalid backend address")
	}
	if conf.FrontendAddr == "" {
		log.Panic("Invalid frontend address")
	}
}
