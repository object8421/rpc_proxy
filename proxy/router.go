//// Copyright 2014 Wandoujia Inc. All Rights Reserved.
//// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"fmt"
	rpc_commons "git.chunyu.me/infra/rpc_commons"
	"git.chunyu.me/infra/rpc_proxy/utils/log"
	zk "git.chunyu.me/infra/rpc_proxy/zk"
	"sync"
	"time"

	"git.chunyu.me/infra/rpc_proxy/utils/errors"
)

type BackService struct {
	ServiceName string
	// 如何处理呢?
	// 可以使用zeromq本身的连接到多个endpoints的特性，自动负载均衡
	backend *BackSockets
	topo    *zk.Topology
	Verbose bool
}

type Router struct {
	mu     sync.Mutex
	pool   map[string]*SharedBackendConn
	closed bool

	sync.RWMutex
	Services        map[string]*BackService
	OfflineServices map[string]*BackService // 在zk中标记下线的
	topo            *zk.Topology
	Verbose         bool
}

// 创建一个BackService
func NewBackService(serviceName string, poller *zmq.Poller, topo *zk.Topology, verbose bool) *BackService {

	backSockets := NewBackSockets(poller, serviceName, verbose)

	service := &BackService{
		ServiceName: serviceName,
		backend:     backSockets,
		poller:      poller,
		topo:        topo,
		Verbose:     verbose,
	}

	var evtbus chan interface{} = make(chan interface{}, 2)
	servicePath := topo.ProductServicePath(serviceName)

	go func() {
		for true {
			endpoints, err := topo.WatchChildren(servicePath, evtbus)

			if err == nil {
				// 如何监听endpoints的变化呢?
				addrSet := make(map[string]bool)
				nowStr := FormatYYYYmmDDHHMMSS(time.Now())
				for _, endpoint := range endpoints {
					// 这些endpoint变化该如何处理呢?
					log.Println(rpc_commons.Green("---->Find Endpoint: "), endpoint, "For Service: ", serviceName)
					endpointInfo, _ := topo.GetServiceEndPoint(serviceName, endpoint)

					addr, ok := endpointInfo[rpc_commons.SERVER_ENDPOINT]
					if ok {
						addrStr := addr.(string)
						log.Println(rpc_commons.Green("---->Add endpoint to backend: "), addrStr, nowStr, "For Service: ", serviceName)
						addrSet[addrStr] = true
					}
				}

				service.backend.UpdateEndpointAddrs(addrSet)

				// 等待事件
				<-evtbus
			} else {
				log.WarnErrorf(err, "zk read failed: %s\n", servicePath)
				// 如果读取失败则，则继续等待5s
				time.Sleep(time.Duration(5) * time.Second)
			}

		}
	}()

	ticker := time.NewTicker(time.Second * 5)
	go func() {
		for _ = range ticker.C {
			service.backend.PurgeEndpoints()
		}
	}()

	return service

}

//
// 将消息发送到Backend上去
//
func (s *BackService) HandleRequest(client_id string, msgs []string) (total int, err error, msg *[]byte) {

	backSocket := s.backend.NextSocket()
	if backSocket == nil {
		// 没有后端服务
		if s.Verbose {
			log.Println(rpc_commons.Red("No BackSocket Found for service:"), s.ServiceName)
		}
		errMsg := GetWorkerNotFoundData(s.ServiceName, 0)
		return 0, nil, &errMsg
	} else {
		if s.Verbose {
			log.Println("SendMessage With: ", backSocket.Addr, "For Service: ", s.ServiceName)
		}
		total, err = backSocket.SendMessage("", client_id, "", msgs)
		return total, err, nil
	}
}

func NewRouter(productName string, topo *zk.Topology, verbose bool) *Router {
	r := &Router{
		pool:            make(map[string]*SharedBackendConn),
		Services:        make(map[string]*BackService),
		OfflineServices: make(map[string]*BackService),
		topo:            topo,
		Verbose:         verbose,
	}

	// 监控服务的变化
	r.WatchServices()

	return r
}

func (bk *Router) ReportServices() {
	bk.RLock()
	log.Info(rpc_commons.Green("Report Service Workers: "))
	for serviceName, service := range bk.Services {
		log.Infof("Service: %s, Worker Count: %d\n", serviceName, service.backend.Active)
	}
	bk.RUnlock()
}

// Router负责监听zk中服务列表的变化
func (bk *Router) WatchServices() {
	var evtbus chan interface{} = make(chan interface{}, 2)

	// 1. 保证Service目录存在，否则会报错
	servicesPath := bk.topo.ProductServicesPath()
	path, e1 := bk.topo.CreateDir(servicesPath)

	fmt.Println("Path: ", path, "error: ", e1)

	go func() {
		for true {
			// 无限监听
			services, err := bk.topo.WatchChildren(servicesPath, evtbus)

			if err == nil {
				// 保证数据更新是有效的
				bk.Lock()
				for _, service := range services {
					log.Println("Service: ", service)
					if _, ok := bk.Services[service]; !ok {
						bk.addBackService(service)
					}
				}
				bk.Unlock()

				// 等待事件
				<-evtbus
			} else {
				log.ErrorErrorf(err, "zk watch error: %s, error: %v\n", servicesPath, err)
				time.Sleep(time.Duration(5) * time.Second)
			}
		}
	}()

	// 读取zk, 等待
	log.Println("ProductName: ", bk.topo.ProductName)
}

// 添加一个后台服务(非线程安全)
func (bk *Router) addBackService(service string) {

	backService, ok := bk.Services[service]
	if !ok {
		backService = NewBackService(service, bk.poller, bk.topo, bk.Verbose)
		bk.Services[service] = backService
	}

}
func (bk *Router) GetBackService(service string) *BackService {
	bk.RLock()
	backService, ok := bk.Services[service]
	bk.RUnlock()

	if ok {
		return backService
	} else {
		return nil
	}
}

//func (s *Router) Close() error {
//	//	s.mu.Lock()
//	//	defer s.mu.Unlock()
//	//	if s.closed {
//	//		return nil
//	//	}
//	//	for i := 0; i < len(s.slots); i++ {
//	//		s.resetSlot(i)
//	//	}
//	//	s.closed = true
//	//	return nil
//}

//var errClosedRouter = errors.New("use of closed router")

//func (s *Router) KeepAlive() error {
//	s.mu.Lock()
//	defer s.mu.Unlock()
//	if s.closed {
//		return errClosedRouter
//	}
//	for _, bc := range s.pool {
//		bc.KeepAlive()
//	}
//	return nil
//}

// 后端如何处理一个Request?
func (s *Router) Dispatch(r *Request) error {
	backService := s.GetBackService(r.Service)
	return backService.HandleRequest(r)
}

//func (s *Router) getBackendConn(addr string) *SharedBackendConn {
//	bc := s.pool[addr]
//	if bc != nil {
//		bc.IncrRefcnt()
//	} else {
//		bc = NewSharedBackendConn(addr, s.auth)
//		s.pool[addr] = bc
//	}
//	return bc
//}

//func (s *Router) putBackendConn(bc *SharedBackendConn) {
//	if bc != nil && bc.Close() {
//		delete(s.pool, bc.Addr())
//	}
//}
