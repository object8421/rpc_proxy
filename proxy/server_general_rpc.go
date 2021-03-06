//// Copyright 2015 Spring Rain Software Compnay LTD. All Rights Reserved.
//// Licensed under the MIT (MIT-LICENSE.txt) license.
package proxy

import (
	"fmt"
	thrift "git.apache.org/thrift.git/lib/go/thrift"
	topozk "git.chunyu.me/infra/go-zookeeper/zk"
	utils "git.chunyu.me/infra/rpc_proxy/utils"
	"git.chunyu.me/infra/rpc_proxy/utils/atomic2"
	"git.chunyu.me/infra/rpc_proxy/utils/log"
	zk "git.chunyu.me/infra/rpc_proxy/zk"
	"os"
	"os/signal"
	"syscall"

	"strings"
	"time"
)

type Server interface {
	Run()
}
type ServerFactorory func(config *utils.Config) Server

//
// Thrift Server的参数
//
type ThriftRpcServer struct {
	ZkAddr          string
	ProductName     string
	ServiceName     string
	FrontendAddr    string
	Topo            *zk.Topology
	Processor       thrift.TProcessor
	Verbose         bool
	lastRequestTime atomic2.Int64
	config          *utils.Config
}

func NewThriftRpcServer(config *utils.Config, processor thrift.TProcessor) *ThriftRpcServer {
	log.Printf("FrontAddr: %s\n", Magenta(config.FrontendAddr))

	return &ThriftRpcServer{
		config:       config,
		ZkAddr:       config.ZkAddr,
		ProductName:  config.ProductName,
		ServiceName:  config.Service,
		FrontendAddr: config.FrontendAddr,
		Verbose:      config.Verbose,
		Processor:    processor,
	}

}

//
// 根据前端的地址生成服务的id
// 例如: 127.0.0.1:5555 --> 127_0_0_1_5555
// "/Users/feiwang/gowork/src/git.chunyu.me/infra/iplocation/aa.sock"
func GetServiceIdentity(frontendAddr string) string {

	fid := strings.Replace(frontendAddr, ".", "_", -1)
	fid = strings.Replace(fid, ":", "_", -1)
	fid = strings.Replace(fid, "/", "", -1)
	if len(fid) > 20 { // unix domain socket
		fid = fid[len(fid)-20 : len(fid)]
	}
	return fid

}

//
// 去ZK注册当前的Service
//
func RegisterService(serviceName, frontendAddr, serviceId string,
	topo *zk.Topology, evtExit chan interface{},
	workDir string, codeUrlVerion string, state *atomic2.Bool, stateChan chan bool) *ServiceEndpoint {

	// 1. 准备数据
	// 记录Service Endpoint的信息
	servicePath := topo.ProductServicePath(serviceName)

	// 确保东西存在
	if ok, _ := topo.Exist(servicePath); !ok {
		topo.CreateDir(servicePath)
	}

	// 用来从zookeeper获取事件
	evtbus := make(chan interface{})

	// 2. 将信息添加到Zk中, 并且监控Zk的状态(如果添加失败会怎么样?)
	endpoint := NewServiceEndpoint(serviceName, serviceId, frontendAddr, workDir, codeUrlVerion)

	// deployPath

	// 为了保证Add是干净的，需要先删除，保证自己才是Owner
	endpoint.DeleteServiceEndpoint(topo)

	// 如果没有状态，或状态为true, 则上线
	if state == nil || state.Get() {
		endpoint.AddServiceEndpoint(topo)
	}

	go func() {

		for true {
			// Watch的目的是监控: 当前的zk session的状态, 如果session出现异常，则重新注册
			_, err := topo.WatchNode(servicePath, evtbus)

			if err == nil {
				// 如果成功添加Watch, 则等待退出，或者ZK Event
				select {
				case <-evtExit:
					return
				case <-stateChan:
					// 如何状态变化(则重新注册)
					endpoint.DeleteServiceEndpoint(topo)
					if state == nil || state.Get() {
						endpoint.AddServiceEndpoint(topo)
					}
				case e := <-evtbus:
					event := e.(topozk.Event)
					if event.State == topozk.StateExpired ||
						event.Type == topozk.EventNotWatching {
						// Session过期了，则需要删除之前的数据，
						// 因为当前的session不是之前的数据的Owner
						endpoint.DeleteServiceEndpoint(topo)

						if state == nil || state.Get() {
							endpoint.AddServiceEndpoint(topo)
						}

					}
				}

			} else {
				// 如果添加Watch失败，则等待退出，或等待timer接触，进行下一次尝试
				timer := time.NewTimer(time.Second * time.Duration(10))
				log.ErrorErrorf(err, "Reg Failed, Wait 10 seconds...., %v", err)
				select {
				case <-evtExit:
					return
				case <-timer.C:
					// pass
				}
			}

		}
	}()
	return endpoint
}

//
// 后端如何处理一个Request
//
func (p *ThriftRpcServer) Dispatch(r *Request) error {
	transport := NewTMemoryBufferWithBuf(r.Request.Data)
	ip := thrift.NewTBinaryProtocolTransport(transport)

	slice := getSlice(0, DEFAULT_SLICE_LEN)
	transport = NewTMemoryBufferWithBuf(slice)
	op := thrift.NewTBinaryProtocolTransport(transport)
	p.Processor.Process(ip, op)

	r.Response.Data = transport.Bytes()

	_, _, seqId, _ := DecodeThriftTypIdSeqId(r.Response.Data)

	log.Debugf("SeqId: %d vs. %d, Dispatch Over", r.Request.SeqId, seqId)
	//	// 如果transport重新分配了内存，则立即归还slice
	//	if cap(r.Response.Data) != DEFAULT_SLICE_LEN {
	//		returnSlice(slice)
	//	}
	return nil
}

func (p *ThriftRpcServer) Run() {
	//	// 1. 创建到zk的连接
	p.Topo = zk.NewTopology(p.ProductName, p.ZkAddr)

	// 127.0.0.1:5555 --> 127_0_0_1:5555
	lbServiceName := GetServiceIdentity(p.FrontendAddr)

	exitSignal := make(chan os.Signal, 1)
	signal.Notify(exitSignal, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL)

	// syscall.SIGKILL
	// kill -9 pid
	// kill -s SIGKILL pid 还是留给运维吧

	StartTicker(p.config.FalconClient, p.ServiceName)

	// 初始状态为不上线
	var state atomic2.Bool
	state.Set(false)
	stateChan := make(chan bool)

	// 注册服务
	evtExit := make(chan interface{})
	endpoint := RegisterService(p.ServiceName, p.FrontendAddr, lbServiceName,
		p.Topo, evtExit, p.config.WorkDir, p.config.CodeUrlVersion,
		&state, stateChan)

	// 3. 读取"前端"的配置
	var transport thrift.TServerTransport
	var err error

	isUnixDomain := false
	// 127.0.0.1:9999(以:区分不同的类型)
	if !strings.Contains(p.FrontendAddr, ":") {
		if FileExist(p.FrontendAddr) {
			os.Remove(p.FrontendAddr)
		}
		transport, err = NewTServerUnixDomain(p.FrontendAddr)
		isUnixDomain = true
	} else {
		transport, err = thrift.NewTServerSocket(p.FrontendAddr)
	}

	if err != nil {
		log.ErrorErrorf(err, Red("Server Socket Create Failed: %v"), err)
		panic(fmt.Sprintf("Invalid FrontendAddr: %s", p.FrontendAddr))
	}

	err = transport.Listen()
	if err != nil {
		log.ErrorErrorf(err, Red("Server Socket Open Failed: %v"), err)
		panic(fmt.Sprintf("Server Socket Open Failed: %s", p.FrontendAddr))
	}

	ch := make(chan thrift.TTransport, 4096)
	defer close(ch)

	// 强制退出? TODO: Graceful退出
	go func() {
		<-exitSignal
		evtExit <- true
		log.Info(Magenta("Receive Exit Signals...."))
		endpoint.DeleteServiceEndpoint(p.Topo)

		// 等待
		start := time.Now().Unix()
		for true {
			// 如果5s内没有接受到新的请求了，则退出
			now := time.Now().Unix()
			if now-p.lastRequestTime.Get() > 5 {
				log.Info(Red("Graceful Exit..."))
				break
			} else {
				log.Printf(Cyan("Sleeping %d seconds\n"), now-start)
				time.Sleep(time.Second)
			}
		}
		transport.Interrupt()
		transport.Close()
	}()

	go func() {
		var address string
		for c := range ch {
			// 为每个Connection建立一个Session
			socket, ok := c.(SocketAddr)

			if ok {
				if isUnixDomain {
					address = p.FrontendAddr
				} else {
					address = socket.Addr().String()
				}
			} else {
				address = "unknow"
			}
			x := NewNonBlockSession(c, address, p.Verbose, &p.lastRequestTime)
			// Session独立处理自己的请求
			go x.Serve(p, 1000)
		}
	}()

	// 准备上线服务
	state.Set(true)
	stateChan <- true

	// Accept什么时候出错，出错之后如何处理呢?
	for {
		c, err := transport.Accept()
		if err != nil {
			break
		} else {
			ch <- c
		}
	}
}
