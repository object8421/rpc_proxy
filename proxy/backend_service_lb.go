package proxy

import (
	thrift "git.apache.org/thrift.git/lib/go/thrift"
	"git.chunyu.me/infra/rpc_proxy/utils/log"
	"sync"
)

//
// Proxy中用来和后端服务通信的模块
//
type BackServiceLB struct {
	serviceName string
	backendAddr string

	sync.RWMutex
	activeConns      []*BackendConnLB // 每一个BackendConn应该有一定的高可用保障
	currentConnIndex int

	verbose bool
	exitEvt chan bool
	ch      chan thrift.TTransport
}

// 创建一个BackService
func NewBackServiceLB(serviceName string, backendAddr string, verbose bool, exitEvt chan bool) *BackServiceLB {

	service := &BackServiceLB{
		serviceName: serviceName,
		backendAddr: backendAddr,
		activeConns: make([]*BackendConnLB, 0, 10),
		verbose:     verbose,
		exitEvt:     exitEvt,
		ch:          make(chan thrift.TTransport, 4096),
	}

	service.run()
	return service

}

//
// 后端如何处理一个Request, 处理完毕之后直接返回，因为Caller已经做好异步处理了
//
func (s *BackServiceLB) Dispatch(r *Request) error {
	backendConn := s.nextBackendConn()

	r.Service = s.serviceName

	if backendConn == nil {
		// 没有后端服务
		if s.verbose {
			log.Printf(Red("No BackSocket Found for service: %s.%s\n"), s.serviceName, r.Request.Name)
		}
		// 从errMsg来构建异常
		errMsg := GetWorkerNotFoundData(r, "BackServiceLB")
		log.Printf(Magenta("---->Convert Error Back to Exception:[%d] %s\n"), len(errMsg), string(errMsg))
		r.Response.Data = errMsg

		return nil
	} else {
		if s.verbose {
			log.Println("SendMessage With: ", backendConn.Addr4Log(), "For Service: ", s.serviceName)
		}
		backendConn.PushBack(r)

		r.Wait.Wait()

		return nil
	}
}

func (s *BackServiceLB) run() {
	// 3. 读取后端服务的配置
	transport, err := thrift.NewTServerSocket(s.backendAddr)
	if err != nil {
		log.ErrorErrorf(err, "Server Socket Create Failed: %v\n", err)
		panic("BackendAddr Invalid")
	}

	err = transport.Open()
	if err != nil {
		log.ErrorErrorf(err, "Server Socket Open Failed: %v\n", err)
		panic("Server Socket Open Failed")
	}

	// 和transport.open做的事情一样，如果Open没错，则Listen也不会有问题
	transport.Listen()

	log.Printf(Green("LB Backend Services listens at: %s\n"), s.backendAddr)

	s.ch = make(chan thrift.TTransport, 4096)

	// 强制退出? TODO: Graceful退出
	go func() {
		<-s.exitEvt
		log.Info(Red("Receive Exit Signals...."))
		transport.Interrupt()
		transport.Close()
	}()

	go func() {
		for c := range s.ch {
			// 为每个Connection建立一个Session
			socket, ok := c.(*thrift.TSocket)
			if ok {
				backendAddr := socket.Addr().String()
				conn := NewBackendConnLB(socket, s.serviceName, backendAddr, s, s.verbose)

				// 因为连接刚刚建立，可靠性还是挺高的，因此直接加入到列表中
				s.Lock()
				// 添加一个Conn到activeConn中
				conn.Index = len(s.activeConns)
				s.activeConns = append(s.activeConns, conn)
				s.Unlock()
				log.Printf(Green("Current Active Conns: %d\n"), conn.Index)
			} else {
				panic("Unexpected Socket Type")
			}
		}
	}()

	// Accept什么时候出错，出错之后如何处理呢?
	go func() {
		for {
			c, err := transport.Accept()
			if err != nil {
				return
			} else {
				s.ch <- c
			}
		}
	}()
}

func (s *BackServiceLB) Active() int {
	return len(s.activeConns)
}

// 获取下一个active状态的BackendConn
func (s *BackServiceLB) nextBackendConn() *BackendConnLB {

	// TODO: 暂时采用RoundRobin的方法，可以采用其他具有优先级排列的方法
	var backSocket *BackendConnLB
	s.RLock()
	if len(s.activeConns) == 0 {
		if s.verbose {
			log.Printf(Cyan("ActiveConns Len 0\n"))
		}
		backSocket = nil
	} else {
		if s.currentConnIndex >= len(s.activeConns) {
			s.currentConnIndex = 0
		}
		backSocket = s.activeConns[s.currentConnIndex]
		s.currentConnIndex++
		if s.verbose {
			log.Printf(Cyan("ActiveConns Len %d, CurrentIndex: %d\n"), len(s.activeConns), s.currentConnIndex)
		}
	}
	s.RUnlock()
	return backSocket
}

// 只有在conn出现错误时才会调用
func (s *BackServiceLB) StateChanged(conn *BackendConnLB) {
	s.Lock()
	if conn.IsConnActive {
		log.Printf(Magenta("Unexpected BackendConnLB State\n"))
		if s.verbose {
			panic("Unexpected BackendConnLB State")
		}
	} else {
		log.Printf(Red("Remove BackendConn From activeConns: %s, Index: %d, Count: %d\n"), conn.Addr4Log(), conn.Index, len(s.activeConns))

		// 从数组中删除一个元素(O(1)的操作)
		if conn.Index != -1 {
			// 1. 和最后一个元素进行交换
			lastIndex := len(s.activeConns) - 1
			if lastIndex != conn.Index {
				lastConn := s.activeConns[lastIndex]

				// 将最后一个元素和当前的元素交换位置
				s.activeConns[conn.Index] = lastConn
				lastConn.Index = conn.Index

				// 删除引用
				s.activeConns[lastIndex] = nil
				conn.Index = -1

			}
			log.Printf(Red("Remove BackendConn From activeConns: %s\n"), conn.Addr4Log())

			// 2. slice
			s.activeConns = s.activeConns[0:lastIndex]

		}
	}
	s.Unlock()
}
