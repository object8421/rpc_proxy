package proxy

import (
	thrift "git.apache.org/thrift.git/lib/go/thrift"
	topozk "git.chunyu.me/infra/go-zookeeper/zk"
	utils "git.chunyu.me/infra/rpc_proxy/utils"
	"git.chunyu.me/infra/rpc_proxy/utils/log"
	zk "git.chunyu.me/infra/rpc_proxy/zk"
	"os"
	"os/signal"
	"syscall"

	"strings"
	"time"
)

const (
	SERVER_ENDPOINT = "frontend"
)

type Server interface {
	Run()
}
type ServerFactorory func(config *utils.Config) Server

//
// Thrift Server的参数
//
type ThriftRpcServer struct {
	ZkAddr       string
	ProductName  string
	ServiceName  string
	FrontendAddr string
	Topo         *zk.Topology
	Processor    thrift.TProcessor
	Verbose      bool
}

func NewThriftRpcServer(config *utils.Config, processor thrift.TProcessor) *ThriftRpcServer {
	log.Printf("FrontAddr: %s\n", Magenta(config.FrontendAddr))

	return &ThriftRpcServer{
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
// 例如: tcp://127.0.0.1:5555 --> tcp_127_0_0_1_5555
//
func GetServiceIdentity(frontendAddr string) string {
	fid := strings.Replace(frontendAddr, ".", "_", -1)
	fid = strings.Replace(fid, ":", "_", -1)
	fid = strings.Replace(fid, "//", "", -1)
	return fid
}

//
// 去ZK注册当前的Service
//
func RegisterService(serviceName, frontendAddr, serviceId string, topo *zk.Topology, evtExit chan interface{}) {
	// 1. 准备数据
	// 记录Service Endpoint的信息
	var endpointInfo map[string]interface{} = make(map[string]interface{})
	endpointInfo[SERVER_ENDPOINT] = frontendAddr
	servicePath := topo.ProductServicePath(serviceName)

	// 用来从zookeeper获取实践
	evtbus := make(chan interface{})

	// 2. 将信息添加到Zk中, 并且监控Zk的状态(如果添加失败会怎么样?)
	topo.AddServiceEndPoint(serviceName, serviceId, endpointInfo)

	go func() {

		for true {
			// 只是为了监控状态
			_, err := topo.WatchNode(servicePath, evtbus)

			if err == nil {
				// 如果成功添加Watch, 则等待退出，或者ZK Event
				select {
				case <-evtExit:
					return
				case e := <-evtbus:
					event := e.(topozk.Event)
					if event.State == topozk.StateExpired || event.Type == topozk.EventNotWatching {
						// Session过期了，则需要删除之前的数据，因为这个数据的Owner不是当前的Session
						topo.DeleteServiceEndPoint(serviceName, serviceId)
						topo.AddServiceEndPoint(serviceName, serviceId, endpointInfo)
					}
				}

			} else {
				// 如果添加Watch失败，则等待退出，或等待timer接触，进行下一次尝试
				timer := time.NewTimer(time.Second * time.Duration(10))
				select {
				case <-evtExit:
					return
				case <-timer.C:
					// pass
				}
			}

		}
	}()
}

// 后端如何处理一个Request?
func (p *ThriftRpcServer) Dispatch(r *Request) error {
	transport := NewTMemoryBufferWithBuf(r.Request.Data)
	ip := thrift.NewTBinaryProtocolTransport(transport)

	transport = NewTMemoryBufferLen(1024)
	op := thrift.NewTBinaryProtocolTransport(transport)
	p.Processor.Process(ip, op)

	r.Response.Data = transport.Bytes()
	return nil
}

func (p *ThriftRpcServer) Run() {
	//	// 1. 创建到zk的连接
	p.Topo = zk.NewTopology(p.ProductName, p.ZkAddr)

	// 127.0.0.1:5555 --> 127_0_0_1:5555
	lbServiceName := GetServiceIdentity(p.FrontendAddr)

	ch1 := make(chan os.Signal, 1)

	signal.Notify(ch1, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL)
	// syscall.SIGKILL
	// kill -9 pid
	// kill -s SIGKILL pid 还是留给运维吧
	//

	// 注册服务
	evtExit := make(chan interface{})
	RegisterService(p.ServiceName, p.FrontendAddr, lbServiceName, p.Topo, evtExit)

	//	var suideTime time.Time

	//	isAlive := true

	// 3. 读取后端服务的配置
	transport, err := thrift.NewTServerSocket(p.FrontendAddr)
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
		p.Topo.DeleteServiceEndPoint(p.ServiceName, lbServiceName)
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
			x := NewNonBlockSession(c, address)
			// Session独立处理自己的请求
			go x.Serve(p, 1000)
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

	//	for {
	//		var sockets []zmq.Polled
	//		var err error

	//		sockets, err = poller1.Poll(time.Second)
	//		if err != nil {
	//			log.Errorf("Error When Pollling: %v\n", err)
	//			continue
	//		}

	//		hasValidMsg := false
	//		for _, socket := range sockets {
	//			switch socket.Socket {
	//			case frontend:
	//				hasValidMsg = true
	//				log.Println("----->Message from front: ")
	//				msgs, err := frontend.RecvMessage(0)
	//				if err != nil {
	//					log.Errorf("Error when reading from frontend: %v\n", err)
	//					continue
	//				}

	//				// msgs:
	//				// <proxy_id, "", client_id, "", rpc_data>
	//				if p.Verbose {
	//					utils.PrintZeromqMsgs(msgs, "frontend")
	//				}
	//				msgs = utils.TrimLeftEmptyMsg(msgs)

	//				bufferIn := thrift.NewTMemoryBufferLen(0)
	//				bufferIn.WriteString(msgs[len(msgs)-1])
	//				procIn := thrift.NewTBinaryProtocolTransport(bufferIn)
	//				bufferOut := thrift.NewTMemoryBufferLen(0)
	//				procOut := thrift.NewTBinaryProtocolTransport(bufferOut)

	//				p.Processor.Process(procIn, procOut)

	//				result := bufferOut.Bytes()
	//				// <proxy_id, "", client_id, "", rpc_data>
	//				frontend.SendMessage(msgs[0:(len(msgs)-1)], result)
	//			}
	//		}

	//		if !isAlive {
	//			if hasValidMsg {
	//				suideTime = time.Now().Add(time.Second * 3)
	//			} else {
	//				if time.Now().After(suideTime) {
	//					log.Println(Green("Load Balance Suiside Gracefully"))
	//					break
	//				}
	//			}
	//		}

	//		// 心跳同步
	//		select {
	//		case sig := <-ch:
	//			if isAlive {
	//				isAlive = false
	//				// 准备退出
	//				evtExit <- ""

	//				// 准备退出(但是需要处理完毕手上的活)

	//				// 需要退出:
	//				p.Topo.DeleteServiceEndPoint(p.ServiceName, lbServiceName)

	//				if sig == syscall.SIGKILL {
	//					log.Println(Red("Got Kill Signal, Return Directly"))
	//					break
	//				} else {
	//					suideTime = time.Now().Add(time.Second * 3)
	//					log.Println(Red("Schedule to suicide at: "), suideTime.Format("@2006-01-02 15:04:05"))
	//				}
	//			}
	//		default:
	//		}
	//	}
}
