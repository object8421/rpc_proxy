package proxy

import (
	"fmt"
	thrift "git.apache.org/thrift.git/lib/go/thrift"
	utils "git.chunyu.me/infra/rpc_proxy/utils"
	zk "git.chunyu.me/infra/rpc_proxy/zk"
	"strings"
	"syscall"
	"time"
)

const (
	HEARTBEAT_INTERVAL = 1000 * time.Millisecond
)

type ProxyServer struct {
	ProductName  string
	FrontendAddr string
	ZkAdresses   string
	Verbose      bool
	Profile      bool
	Router       *Router
}

func NewProxyServer(config *utils.Config) *ProxyServer {
	server := &ProxyServer{
		ProductName:  config.ProductName,
		FrontendAddr: config.ProxyAddr,
		ZkAdresses:   config.ZkAddr,
		Verbose:      config.Verbose,
		Profile:      config.Profile,
		Router:       NewRouter(),
	}
	return server
}

//
// 两参数是必须的:  ProductName, zkAddress, frontAddr可以用来测试
//
func (p *ProxyServer) Run() {
	// 1. 创建到zk的连接
	var topo *zk.Topology
	topo = zk.NewTopology(p.ProductName, p.ZkAdresses)

	// 3. 读取后端服务的配置

	transport := thrift.NewTServerSocket(p.FrontendAddr)

	// 开始监听
	transport.Listen()

	ch := make(chan thrift.TTransport, 4096)
	defer close(ch)

	go func() {
		for c := range ch {
			// 为每个Connection建立一个Session
			x := NewSession(c)
			// Session独立处理自己的请求
			go x.Serve(p.Router, 1000)
		}
	}()

	NewBackServices

	// Accept什么时候出错，出错之后如何处理呢?
	for {
		c, err := transport.Accept()
		if err != nil {
			return
		} else {
			ch <- c
		}
	}
}

func printList(msgs []string) string {
	results := make([]string, 0, len(msgs))
	results = append(results, fmt.Sprintf("Msgs Len: %d, ", len(msgs)-1))
	for i := 0; i < len(msgs)-1; i++ {
		results = append(results, fmt.Sprintf("[%s]", msgs[i]))
	}
	return strings.Join(results, ",")
}
