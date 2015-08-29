package proxy

import (
	"git.chunyu.me/infra/rpc_proxy/utils/log"
	"sync"
	"time"
)

//
// Proxy中用来和后端服务通信的模块
//
type BackServiceLB struct {
	ServiceName string
	BackendAddr string

	sync.RWMutex
	activeConns      []*BackendConnLB // 每一个BackendConn应该有一定的高可用保障
	CurrentConnIndex int

	Verbose bool
	exitEvt chan bool
}

// 创建一个BackService
func NewBackServiceLB(serviceName string, backendAddr string, verbose bool, exitEvt chan bool) *BackServiceLB {

	service := &BackServiceLB{
		ServiceName: serviceName,
		BackendAddr: backendAddr,
		activeConns: make([]*BackendConnLB, 0, 10),
		Verbose:     verbose,
		exitEvt:     exitEvt,
	}
	service.Run()
	return service

}

func (s *BackServiceLB) Run() {
	// 3. 读取后端服务的配置
	transport, err := thrift.NewTServerSocket(s.BackendAddr)
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
		<-exitEvt
		log.Info(Green("Receive Exit Signals...."))
		transport.Interrupt()
		transport.Close()
	}()

	go func() {
		var address string
		for c := range ch {
			// 为每个Connection建立一个Session
			socket, ok := c.(*thrift.TSocket)
			// 会自动加入到active中
			NewBackendConnLB(socket, s)
		}
	}()

	// Accept什么时候出错，出错之后如何处理呢?
	go func() {
		for {
			c, err := transport.Accept()
			if err != nil {
				return
			} else {
				ch <- c
			}
		}
	}()
}

func (s *BackServiceLB) Active() int {
	return len(s.activeConns)
}

// 获取下一个active状态的BackendConn
func (s *BackServiceLB) NextBackendConn() *BackendConn {

	// TODO: 暂时采用RoundRobin的方法，可以采用其他具有优先级排列的方法
	var backSocket *BackServiceLB
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
func (s *BackServiceLB) HandleRequest(req *Request) (err error) {
	backendConn := s.NextBackendConn()

	if backendConn == nil {
		// 没有后端服务
		if s.Verbose {
			log.Println(Red("No BackSocket Found for service:"), s.ServiceName)
		}
		// 从errMsg来构建异常
		errMsg := GetWorkerNotFoundData(s.ServiceName, 0)
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

func (s *BackServiceLB) StateChanged(conn *BackendConnLB) {
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
