package proxy

import (
	thrift "git.apache.org/thrift.git/lib/go/thrift"
	utils "git.chunyu.me/infra/rpc_proxy/utils"
	"git.chunyu.me/infra/rpc_proxy/utils/log"
	zk "git.chunyu.me/infra/rpc_proxy/zk"
	"os"
	"os/signal"
	"syscall"
)

//
// Thrift Server的参数
//
type ThriftLoadBalanceServer struct {
	productName    string
	serviceName    string
	frontendAddr   string // 绑定的端口
	backendAddr    string
	lbServiceName  string
	topo           *zk.Topology // ZK相关
	zkAddr         string
	verbose        bool
	backendService *BackServiceLB
	exitEvt        chan bool
}

func NewThriftLoadBalanceServer(config *utils.Config) *ThriftLoadBalanceServer {
	log.Printf("FrontAddr: %s\n", Magenta(config.FrontendAddr))

	// 前端对接rpc_proxy
	p := &ThriftLoadBalanceServer{
		zkAddr:       config.ZkAddr,
		productName:  config.ProductName,
		serviceName:  config.Service,
		frontendAddr: config.FrontendAddr,
		backendAddr:  config.BackAddr,
		verbose:      config.Verbose,
		exitEvt:      make(chan bool),
	}

	p.topo = zk.NewTopology(p.productName, p.zkAddr)
	p.lbServiceName = GetServiceIdentity(p.frontendAddr)

	// 后端对接: 各种python的rpc server
	p.backendService = NewBackServiceLB(p.serviceName, p.backendAddr, p.verbose, p.exitEvt)
	return p

}

func (p *ThriftLoadBalanceServer) Run() {
	//	// 1. 创建到zk的连接

	// 127.0.0.1:5555 --> 127_0_0_1:5555

	ch1 := make(chan os.Signal, 1)

	signal.Notify(ch1, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL)
	// syscall.SIGKILL
	// kill -9 pid
	// kill -s SIGKILL pid 还是留给运维吧
	//

	// 注册服务
	evtExit := make(chan interface{})
	RegisterService(p.serviceName, p.frontendAddr, p.lbServiceName, p.topo, evtExit)

	//	var suideTime time.Time

	//	isAlive := true

	// 3. 读取后端服务的配置
	transport, err := thrift.NewTServerSocket(p.frontendAddr)
	if err != nil {
		log.ErrorErrorf(err, "Server Socket Create Failed: %v\n", err)
	}

	transport.Open()

	// 开始监听
	transport.Listen()

	ch := make(chan thrift.TTransport, 4096)
	defer close(ch)

	// 强制退出? TODO: Graceful退出
	go func() {
		<-ch1
		log.Info(Green("Receive Exit Signals...."))
		p.topo.DeleteServiceEndPoint(p.serviceName, p.lbServiceName)
		transport.Interrupt()
		transport.Close()
	}()

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
			x := NewNonBlockSession(c, address, p.verbose)
			// Session独立处理自己的请求
			go x.Serve(p.backendService, 1000)
		}
	}()

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
