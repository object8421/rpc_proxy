package proxy

import (
	"git.chunyu.me/infra/rpc_proxy/utils/log"
	zk "git.chunyu.me/infra/rpc_proxy/zk"
	"sync"
	"time"
)

//
// Proxy中用来和后端服务通信的模块
//
type BackService struct {
	ServiceName string
	topo        *zk.Topology

	sync.RWMutex
	activeConns      []*BackendConn // 每一个BackendConn应该有一定的高可用保障
	CurrentConnIndex int
	addr2Conn        map[string]*BackendConn

	Verbose bool
}

// 创建一个BackService
func NewBackService(serviceName string, topo *zk.Topology, verbose bool) *BackService {

	service := &BackService{
		ServiceName: serviceName,
		activeConns: make([]*BackendConn, 0, 10),
		addr2Conn:   make(map[string]*BackendConn),
		topo:        topo,
		Verbose:     verbose,
	}

	service.WatchBackServiceNodes()

	return service

}

func (s *BackService) Active() int {
	return len(s.activeConns)
}

//
// 如何处理后端服务的变化呢?
//
func (s *BackService) WatchBackServiceNodes() {
	var evtbus chan interface{} = make(chan interface{}, 2)
	servicePath := s.topo.ProductServicePath(s.ServiceName)

	go func() {
		for true {
			endpoints, err := s.topo.WatchChildren(servicePath, evtbus)

			if err == nil {
				// 如何监听endpoints的变化呢?
				addressList := make([]string, 0, 10)
				nowStr := FormatYYYYmmDDHHMMSS(time.Now())
				for _, endpoint := range endpoints {
					// 这些endpoint变化该如何处理呢?
					log.Println(Green("---->Find Endpoint: "),
						endpoint, "For Service: ", s.ServiceName)
					endpointInfo, _ := s.topo.GetServiceEndPoint(s.ServiceName, endpoint)

					addr, ok := endpointInfo[SERVER_ENDPOINT]
					if ok {
						addrStr := addr.(string)
						log.Println(Green("---->Add endpoint to backend: "),
							addrStr, nowStr, "For Service: ", s.ServiceName)
						addressList = append(addressList, addrStr)
					}
				}

				// 如何更新BackendConn呢?
				s.Lock()
				for _, addr := range addressList {

					conn, ok := s.addr2Conn[addr]
					if ok {
						switch conn.State {
						case ConnStateActive:
							// 什么也不做
						case ConnStateDied, ConnStateMarkOffline:
							conn.State = ConnStateInit
							// TODO: 重新开始连接
						}
					} else {
						conn := NewBackendConn(addr, s)
						s.addr2Conn[addr] = conn
						log.Printf(Red("Add BackendConn to activeConns: %s\n"), addr)
						//						s.RLock()
						s.activeConns = append(s.activeConns, conn)
						//						s.RUnLock()
					}
				}
				s.Unlock()

				// 等待事件
				<-evtbus
			} else {
				log.WarnErrorf(err, "zk read failed: %s\n", servicePath)
				// 如果读取失败则，则继续等待5s
				time.Sleep(time.Duration(5) * time.Second)
			}

		}
	}()
}

// 获取下一个active状态的BackendConn
func (s *BackService) NextBackendConn() *BackendConn {
	var backSocket *BackendConn
	s.RLock()
	if len(s.activeConns) == 0 {
		backSocket = nil
	} else {
		if s.CurrentConnIndex >= len(s.activeConns) {
			s.CurrentConnIndex = 0
		}
		backSocket = s.activeConns[s.CurrentConnIndex]
		s.CurrentConnIndex++
	}
	s.RUnlock()
	return backSocket
}

//
// 将消息发送到Backend上去
//
func (s *BackService) HandleRequest(req *Request) (err error) {
	backendConn := s.NextBackendConn()

	if backendConn == nil {
		// 没有后端服务
		if s.Verbose {
			log.Println(Red("No BackSocket Found for service:"), s.ServiceName)
		}
		// 从errMsg来构建异常
		errMsg := GetWorkerNotFoundData(req)
		req.Response.Data = errMsg
		//		req.Wait.Done()

		return nil
	} else {
		if s.Verbose {
			log.Println("SendMessage With: ", backendConn.Addr(), "For Service: ", s.ServiceName)
		}
		backendConn.PushBack(req)
		return nil
	}
}

func (s *BackService) StateChanged(conn *BackendConn) {
	s.Lock()
	if conn.State == ConnStateActive {
		conn.Index = len(s.activeConns)
		log.Printf(Red("Add BackendConn to activeConns: %s\n"), conn.Addr())
		s.activeConns = append(s.activeConns, conn)
	} else {
		if conn.Index != -1 {
			lastIndex := len(s.activeConns) - 1
			if lastIndex != conn.Index {
				lastConn := s.activeConns[lastIndex]
				// 将最后一个元素和当前的元素交换位置
				s.activeConns[conn.Index] = lastConn
				lastConn.Index = conn.Index
				conn.Index = -1

				// slice
				s.activeConns = s.activeConns[0:lastIndex]

				log.Printf(Red("Remove BackendConn From activeConns: %s\n"), conn.Addr())

			}
		}
	}
	s.Unlock()
}
