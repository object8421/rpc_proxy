package lb

import (
	rpc_commons "git.chunyu.me/infra/rpc_commons"
	proxy "git.chunyu.me/infra/rpc_proxy/proxy"
	utils "git.chunyu.me/infra/rpc_proxy/utils"
	"git.chunyu.me/infra/rpc_proxy/utils/log"
	zk "git.chunyu.me/infra/rpc_proxy/zk"
	zmq "github.com/pebbe/zmq4"

	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	PPP_READY_BYTE     = uint8('\x01') // 通知lb, Worker Ready
	PPP_HEARTBEAT_BYTE = uint8('\x02') // 通知lb, Worker 还活着
	PPP_HEARTBEAT_STR  = "\x02"
	PPP_STOP_BYTE      = uint8('\x03') // 通知lb, Worker 即将关闭，如果有什么Event请不要再分配了

)

type LoadBalanceServer struct {
	ProductName  string
	ServiceName  string
	FrontendAddr string
	BackendAddr  string
	ZkAddr       string
	Verbose      bool
}

func NewLoadBalanceServer(config *utils.Config) *LoadBalanceServer {
	server := &LoadBalanceServer{
		ProductName:  config.ProductName,
		ServiceName:  config.Service,
		FrontendAddr: config.FrontendAddr,
		BackendAddr:  config.BackAddr,
		ZkAddr:       config.ZkAddr,
		Verbose:      config.Verbose,
	}
	return server
}

func (p *LoadBalanceServer) Run() {
	// 1. 创建到zk的连接
	var topo *zk.Topology
	topo = zk.NewTopology(p.ProductName, p.ZkAddr)

	// 2. 启动服务
	frontend, _ := zmq.NewSocket(zmq.ROUTER)
	backend, _ := zmq.NewSocket(zmq.ROUTER)
	defer frontend.Close()
	defer backend.Close()

	// ROUTER/ROUTER绑定到指定的端口

	// tcp://127.0.0.1:5555 --> tcp://127_0_0_1:5555
	lbServiceName := rpc_commons.GetServiceIdentity(p.FrontendAddr)

	frontend.SetIdentity(lbServiceName)
	frontend.Bind(p.FrontendAddr) //  For clients "tcp://*:5555"
	backend.Bind(p.BackendAddr)   //  For workers "tcp://*:5556"

	log.Printf("FrontAddr: %s, BackendAddr: %s\n", rpc_commons.Magenta(p.FrontendAddr), rpc_commons.Magenta(p.BackendAddr))

	// 后端的workers queue
	workersQueue := NewPPQueue()

	// 心跳间隔1s
	heartbeat_at := time.Tick(HEARTBEAT_INTERVAL)

	poller1 := zmq.NewPoller()
	poller1.Add(backend, zmq.POLLIN)

	poller2 := zmq.NewPoller()
	// 前提:
	//     1. 当zeromq通知消息可读时，那么整个Message(所有的msg parts)都可读
	//	   2. 往zeromq写数据时，是异步的，因此也不存在block(除非数据量巨大)
	//
	poller2.Add(backend, zmq.POLLIN)
	poller2.Add(frontend, zmq.POLLIN)

	isAlive := true
	// 注册服务
	evtExit := make(chan interface{})
	rpc_commons.RegisterService(p.ServiceName, p.FrontendAddr, lbServiceName, topo, evtExit)

	ch := make(chan os.Signal, 1)

	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL)
	// syscall.SIGKILL
	// kill -9 pid
	// kill -s SIGKILL pid 还是留给运维吧
	//

	// 自动退出条件:
	var suideTime time.Time

	for {
		var sockets []zmq.Polled
		var err error

		sockets, err = poller2.Poll(HEARTBEAT_INTERVAL)
		if err != nil {
			// TODO: 似乎Poller自己会监听kill -15等信号，如何处理呢?
			//			break //  Interrupted
			log.Errorf("Error When Pollling: %v\n", err)
			continue
		}

		hasValidMsg := false
		for _, socket := range sockets {
			switch socket.Socket {
			case backend:
				// 格式:
				// 后端:
				// 	             <"", proxy_id, "", client_id, "", rpc_data>
				// Backend Socket读取到的:
				//		<wokerid, "", proxy_id, "", client_id, "", rpc_data>
				//
				msgs, err := backend.RecvMessage(0)
				if err != nil {
					log.Errorf("Error When RecvMessage from background: %v\n", err)
					continue
				}
				if p.Verbose {
					// log.Println("Message from backend: ", msgs)
				}
				// 消息类型:
				// msgs: <worker_id, "", proxy_id, "", client_id, "", rpc_data>
				//       <worker_id, "", rpc_control_data>
				worker_id, msgs := utils.Unwrap(msgs)

				// rpc_control_data 控制信息
				// msgs: <rpc_control_data>
				if len(msgs) == 1 {
					// PPP_READY_BYTE
					// PPP_HEARTBEAT_BYTE
					controlMsg := msgs[0]

					// 碰到无效的信息，则直接跳过去
					if len(controlMsg) == 0 {
						continue
					}
					if p.Verbose {
						// log.Println("Got Message From Backend...")
					}

					if controlMsg[0] == PPP_READY_BYTE || controlMsg[0] == PPP_HEARTBEAT_BYTE {
						// 后端服务剩余的并发能力
						var concurrency int
						if len(controlMsg) >= 3 {
							concurrency = int(controlMsg[2])
						} else {
							concurrency = 1
						}
						if p.Verbose {
							// utils.PrintZeromqMsgs(msgs, "control msg")
						}

						force_update := controlMsg[0] == PPP_READY_BYTE
						workersQueue.UpdateWorkerStatus(worker_id, concurrency, force_update)
					} else if controlMsg[0] == PPP_STOP_BYTE {
						// 停止指定的后端服务
						workersQueue.UpdateWorkerStatus(worker_id, -1, true)
					} else {
						log.Errorf("Unexpected Control Message: %d", controlMsg[0])
					}
				} else {
					hasValidMsg = true
					// 将信息发送到前段服务, 如果前端服务挂了，则消息就丢失
					//					log.Println("Send Message to frontend")
					workersQueue.UpdateWorkerStatus(worker_id, 0, false)
					// msgs: <proxy_id, "", client_id, "", rpc_data>
					frontend.SendMessage(msgs)
				}
			case frontend:
				hasValidMsg = true
				log.Println("----->Message from front: ")
				msgs, err := frontend.RecvMessage(0)
				if err != nil {
					log.Errorf("Error when reading from frontend: %v\n", err)
					continue
				}

				// msgs:
				// <proxy_id, "", client_id, "", rpc_data>
				if p.Verbose {
					utils.PrintZeromqMsgs(msgs, "frontend")
				}
				msgs = utils.TrimLeftEmptyMsg(msgs)

				// 将msgs交给后端服务器
				worker := workersQueue.NextWorker()
				if worker != nil {
					if p.Verbose {
						log.Println("Send Msg to Backend worker: ", worker.Identity)
					}
					backend.SendMessage(worker.Identity, "", msgs)
				} else {
					// 怎么返回错误消息呢?
					if p.Verbose {
						log.Println("No backend worker found")
					}
					errMsg := proxy.GetWorkerNotFoundData("account", 0)

					// <proxy_id, "", client_id, "", rpc_data>
					frontend.SendMessage(msgs[0:(len(msgs)-1)], errMsg)
				}
			}
		}

		// 如果安排的suiside, 则需要处理 suiside的时间
		if !isAlive {
			if hasValidMsg {
				suideTime = time.Now().Add(time.Second * 3)
			} else {
				if time.Now().After(suideTime) {
					log.Println(rpc_commons.Green("Load Balance Suiside Gracefully"))
					break
				}
			}
		}

		// 心跳同步
		select {
		case <-heartbeat_at:
			now := time.Now()

			// 给workerQueue中的所有的worker发送心跳消息
			for _, worker := range workersQueue.WorkerQueue {
				if worker.Expire.After(now) {
					//					log.Println("Sending Hb to Worker: ", worker.Identity)
					backend.SendMessage(worker.Identity, "", PPP_HEARTBEAT_STR)
				}
			}

			workersQueue.PurgeExpired()
		case sig := <-ch:

			if isAlive {
				isAlive = false
				evtExit <- true
				// 准备退出(但是需要处理完毕手上的活)
				// 需要退出:
				topo.DeleteServiceEndPoint(p.ServiceName, lbServiceName)

				if sig == syscall.SIGKILL {
					log.Println(rpc_commons.Red("Got Kill Signal, Return Directly"))
					break
				} else {
					suideTime = time.Now().Add(time.Second * 3)
					log.Println(rpc_commons.Red("Schedule to suicide at: "), proxy.FormatYYYYmmDDHHMMSS(suideTime))
				}
			}
		default:
		}
	}
}
