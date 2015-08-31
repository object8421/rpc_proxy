//// Copyright 2014 Wandoujia Inc. All Rights Reserved.
//// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"errors"
	"fmt"
	"git.chunyu.me/infra/rpc_proxy/utils/log"
	zk "git.chunyu.me/infra/rpc_proxy/zk"
	"sync"
	"time"
)

type Router struct {
	mu sync.Mutex
	//	pool   map[string]*SharedBackendConn
	closed bool

	sync.RWMutex
	services map[string]*BackService
	topo     *zk.Topology
	Verbose  bool
}

func NewRouter(productName string, topo *zk.Topology, verbose bool) *Router {
	r := &Router{
		services: make(map[string]*BackService),
		topo:     topo,
		Verbose:  verbose,
	}

	// 监控服务的变化
	r.WatchServices()

	return r
}

//
// 后端如何处理一个Request
//
func (s *Router) Dispatch(r *Request) error {
	backService := s.GetBackService(r.Service)
	if backService == nil {
		r.Response.Data = GetServiceNotFoundData(r)
		return nil
	} else {
		return backService.HandleRequest(r)
	}
}

//
// 打印当前的Service的情况
//
func (bk *Router) ReportServices() {
	bk.RLock()
	log.Info(Green("Report Service Workers: "))
	for serviceName, service := range bk.services {
		log.Infof("Service: %s, Worker Count: %d\n", serviceName, service.Active())
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
				bk.Lock()
				// 保证数据更新是有效的
				oldServices := bk.services
				bk.services = make(map[string]*BackService, len(services))
				for _, service := range services {
					log.Println("Found Service: ", service)

					back, ok := oldServices[service]
					if ok {
						bk.services[service] = back
						delete(oldServices, service)
					} else {

						bk.addBackService(service)
					}
				}
				bk.Unlock()

				if len(oldServices) > 0 {
					go func() {
						for len(oldServices) > 0 {
							// 遍历，并且关闭
							// TODO:
						}
					}()
				}

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

	backService, ok := bk.services[service]
	if !ok {
		backService = NewBackService(service, bk.topo, bk.Verbose)
		bk.services[service] = backService
	}

}
func (bk *Router) GetBackService(service string) *BackService {
	bk.RLock()
	backService, ok := bk.services[service]
	bk.RUnlock()

	if ok {
		return backService
	} else {
		return nil
	}
}

func (s *Router) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return nil
}

var errClosedRouter = errors.New("use of closed router")

func (s *Router) KeepAlive() error {
	//	s.mu.Lock()
	//	defer s.mu.Unlock()
	//	if s.closed {
	//		return errClosedRouter
	//	}
	//	for _, bc := range s.pool {
	//		bc.KeepAlive()
	//	}
	return nil
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
