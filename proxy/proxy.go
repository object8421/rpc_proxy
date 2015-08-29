package proxy

import (
	"fmt"
	thrift "git.apache.org/thrift.git/lib/go/thrift"
	utils "git.chunyu.me/infra/rpc_proxy/utils"
	"git.chunyu.me/infra/rpc_proxy/utils/log"
	zk "git.chunyu.me/infra/rpc_proxy/zk"
	"strings"
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

	p.Router = NewRouter(p.ProductName, topo, p.Verbose)

	// 3. 读取后端服务的配置

	transport, err := thrift.NewTServerSocket(p.FrontendAddr)
	if err != nil {
		log.ErrorErrorf(err, "Server Socket Create Failed: %v, Front: %s\n", err, p.FrontendAddr)
	}

	// 开始监听
	//	transport.Open()
	transport.Listen()

	ch := make(chan thrift.TTransport, 4096)
	defer close(ch)

	go func() {
		var address string
		for c := range ch {
			// 为每个Connection建立一个Session
			socket, ok := c.(*thrift.TSocket)

			if ok {
				address = socket.Addr().String()
			} else {
				address = "unknow"
			}
			x := NewSession(c, address)
			// Session独立处理自己的请求
			go x.Serve(p.Router, 1000)
		}
	}()

	// Accept什么时候出错，出错之后如何处理呢?
	for {
		c, err := transport.Accept()
		if err != nil {
			log.ErrorErrorf(err, "Accept Error: %v\n", err)
			break
		} else {
			ch <- c
		}
	}

	fmt.Println("")
	select {}
}

func printList(msgs []string) string {
	results := make([]string, 0, len(msgs))
	results = append(results, fmt.Sprintf("Msgs Len: %d, ", len(msgs)-1))
	for i := 0; i < len(msgs)-1; i++ {
		results = append(results, fmt.Sprintf("[%s]", msgs[i]))
	}
	return strings.Join(results, ",")
}
