package proxy

import (
	"fmt"
	rpc_commons "git.chunyu.me/infra/rpc_commons"
	utils "git.chunyu.me/infra/rpc_proxy/utils"
	"git.chunyu.me/infra/rpc_proxy/utils/log"
	zk "git.chunyu.me/infra/rpc_proxy/zk"
	zmq "github.com/pebbe/zmq4"
	"sync"
	"time"
)

type BackService struct {
	ServiceName string
	// 如何处理呢?
	// 可以使用zeromq本身的连接到多个endpoints的特性，自动负载均衡
	backend *BackSockets
	poller  *zmq.Poller
	topo    *zk.Topology
	Verbose bool
}

// 创建一个BackService
func NewBackService(serviceName string, poller *zmq.Poller, topo *zk.Topology, verbose bool) *BackService {

	backSockets := NewBackSockets(poller)

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

			if err != nil {
				// 如何监听endpoints的变化呢?
				addrSet := make(map[string]bool)
				nowStr := FormatYYYYmmDDHHMMSS(time.Now())
				for _, endpoint := range endpoints {
					// 这些endpoint变化该如何处理呢?
					log.Println(utils.Green("---->Find Endpoint: "), endpoint, "For Service: ", serviceName)
					endpointInfo, _ := topo.GetServiceEndPoint(serviceName, endpoint)

					addr, ok := endpointInfo[rpc_commons.SERVER_ENDPOINT]
					if ok {
						addrStr := addr.(string)
						log.Println(utils.Green("---->Add endpoint to backend: "), addrStr, nowStr, "For Service: ", serviceName)
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
			log.Println(utils.Red("No BackSocket Found for service:"), s.ServiceName)
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

// BackServices通过topology来和zk进行交互
type BackServices struct {
	sync.RWMutex
	Services map[string]*BackService

	// 在zk中标记下线的
	OfflineServices map[string]*BackService

	poller *zmq.Poller
	topo   *zk.Topology

	Verbose bool
}

func NewBackServices(poller *zmq.Poller, productName string, topo *zk.Topology, verbose bool) *BackServices {

	// 创建BackServices
	result := &BackServices{
		Services:        make(map[string]*BackService),
		OfflineServices: make(map[string]*BackService),
		poller:          poller,
		topo:            topo,
		Verbose:         verbose,
	}

	var evtbus chan interface{} = make(chan interface{}, 2)
	servicesPath := topo.ProductServicesPath()
	path, e1 := topo.CreateDir(servicesPath) // 保证Service目录存在，否则会报错
	fmt.Println("Path: ", path, "error: ", e1)
	services, err := topo.WatchChildren(servicesPath, evtbus)
	if err != nil {
		log.Println("Error: ", err)
		// TODO: 这个地方需要优化
		panic("Reading Service List Failed")
	}

	go func() {
		for true {

			result.Lock()
			for _, service := range services {
				log.Println("Service: ", service)
				if _, ok := result.Services[service]; !ok {
					result.addBackService(service)
				}
			}
			result.Unlock()

			// 等待事件
			<-evtbus
			// 读取数据，继续监听(连接过期了就过期了，再次Watch即可)
			services, err = topo.WatchChildren(servicesPath, evtbus)
		}
	}()

	// 读取zk, 等待
	log.Println("ProductName: ", result.topo.ProductName)

	return result
}

// 添加一个后台服务
func (bk *BackServices) addBackService(service string) {

	backService, ok := bk.Services[service]
	if !ok {
		backService = NewBackService(service, bk.poller, bk.topo, bk.Verbose)
		bk.Services[service] = backService
	}

}
func (bk *BackServices) GetBackService(service string) *BackService {
	bk.RLock()
	backService, ok := bk.Services[service]
	bk.RUnlock()

	if ok {
		return backService
	} else {
		return nil
	}
}
