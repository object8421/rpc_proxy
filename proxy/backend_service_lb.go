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
	ServiceName string
	BackendAddr string

	sync.RWMutex
	activeConns      []*BackendConnLB // 每一个BackendConn应该有一定的高可用保障
	CurrentConnIndex int

	Verbose bool
	exitEvt chan bool
	ch      chan thrift.TTransport
}

// 创建一个BackService
func NewBackServiceLB(serviceName string, backendAddr string, verbose bool, exitEvt chan bool) *BackServiceLB {

	service := &BackServiceLB{
		ServiceName: serviceName,
		BackendAddr: backendAddr,
		activeConns: make([]*BackendConnLB, 0, 10),
		Verbose:     verbose,
		exitEvt:     exitEvt,
		ch:          make(chan thrift.TTransport, 4096),
	}

	service.Run()
	return service

}

//
// 后端如何处理一个Request, 处理完毕之后直接返回，因为Caller已经做好异步处理了
//
func (s *BackServiceLB) Dispatch(r *Request) error {
	backendConn := s.NextBackendConn()

	if backendConn == nil {
		// 没有后端服务
		if s.Verbose {
			log.Println(Red("No BackSocket Found for service:"), s.ServiceName)
		}
		// 从errMsg来构建异常
		errMsg := GetWorkerNotFoundData(r)
		log.Printf(Magenta("---->Convert Error Back to Exception:[%d] %s\n"), len(errMsg), string(errMsg))
		r.Response.Data = errMsg

		return nil
	} else {
		if s.Verbose {
			log.Println("SendMessage With: ", backendConn.Addr4Log(), "For Service: ", s.ServiceName)
		}
		backendConn.PushBack(r)

		r.Wait.Wait()

		return nil
	}
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

	log.Printf(Green("LB Backend Services listens at: %s\n"), s.BackendAddr)

	if s.ch != nil {
		defer close(s.ch)
		s.ch = nil
	}
	s.ch = make(chan thrift.TTransport, 4096)

	// 强制退出? TODO: Graceful退出
	go func() {
		<-s.exitEvt
		log.Info(Green("Receive Exit Signals...."))
		transport.Interrupt()
		transport.Close()
	}()

	go func() {
		for c := range s.ch {
			// 为每个Connection建立一个Session
			socket, ok := c.(*thrift.TSocket)
			// 会自动加入到active中
			if ok {
				conn := NewBackendConnLB(socket, socket.Addr().String(), s)
				s.Lock()
				s.activeConns = append(s.activeConns, conn)
				s.Unlock()
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
func (s *BackServiceLB) NextBackendConn() *BackendConnLB {

	// TODO: 暂时采用RoundRobin的方法，可以采用其他具有优先级排列的方法
	var backSocket *BackendConnLB
	s.RLock()
	if len(s.activeConns) == 0 {
		log.Printf(Cyan("ActiveConns Len 0\n"))
		backSocket = nil
	} else {
		if s.CurrentConnIndex >= len(s.activeConns) {
			s.CurrentConnIndex = 0
		}
		backSocket = s.activeConns[s.CurrentConnIndex]
		s.CurrentConnIndex++

		log.Printf(Cyan("ActiveConns Len %d, CurrentIndex: %s\n"), len(s.activeConns), s.CurrentConnIndex)
	}
	s.RUnlock()
	return backSocket
}

func (s *BackServiceLB) StateChanged(conn *BackendConnLB) {
	return
	s.Lock()
	if conn.State == ConnStateActive {
		conn.Index = len(s.activeConns)
		log.Printf(Red("Add BackendConn to activeConns: %s\n"), conn.Addr4Log())
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

			}
			log.Printf(Red("Remove BackendConn From activeConns: %s\n"), conn.Addr4Log())
			// slice
			s.activeConns = s.activeConns[0:lastIndex]

		}
	}
	s.Unlock()
}
