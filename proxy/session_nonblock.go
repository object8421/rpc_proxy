//// Copyright 2015 Spring Rain Software Compnay LTD. All Rights Reserved.
//// Licensed under the MIT (MIT-LICENSE.txt) license.
package proxy

import (
	thrift "git.apache.org/thrift.git/lib/go/thrift"
	"git.chunyu.me/infra/rpc_proxy/utils/atomic2"
	"git.chunyu.me/infra/rpc_proxy/utils/errors"
	"git.chunyu.me/infra/rpc_proxy/utils/log"
	"sync"
	"time"
)

//
// 用于rpc proxy或者load balance用来管理Client的
//
type NonBlockSession struct {
	*TBufferedFramedTransport

	RemoteAddress string
	Ops           int64
	LastOpUnix    int64
	CreateUnix    int64

	quit    bool
	failed  atomic2.Bool
	closed  atomic2.Bool
	verbose bool

	lastRequestTime *atomic2.Int64
}

func NewNonBlockSession(c thrift.TTransport, address string, verbose bool,
	lastRequestTime *atomic2.Int64) *NonBlockSession {
	return NewNonBlockSessionSize(c, address, verbose, lastRequestTime, 1024*32, 1800)
}

func NewNonBlockSessionSize(c thrift.TTransport, address string, verbose bool,
	lastRequestTime *atomic2.Int64, bufsize int, timeout int) *NonBlockSession {
	s := &NonBlockSession{
		CreateUnix:               time.Now().Unix(),
		RemoteAddress:            address,
		lastRequestTime:          lastRequestTime,
		verbose:                  verbose,
		TBufferedFramedTransport: NewTBufferedFramedTransport(c, time.Microsecond*100, 20),
	}

	// 还是基于c net.Conn进行读写，只是采用Redis协议进行编码解码
	// Reader 处理Client发送过来的消息
	// Writer 将后端服务的数据返回给Client
	log.Printf(Green("Session From Proxy [%s] created"), address)
	return s
}

func (s *NonBlockSession) Close() error {
	s.failed.Set(true)
	s.closed.Set(true)
	return s.TBufferedFramedTransport.Close()
}

func (s *NonBlockSession) IsClosed() bool {
	return s.closed.Get()
}

func (s *NonBlockSession) Serve(d Dispatcher, maxPipeline int) {
	var errlist errors.ErrorList

	defer func() {
		// 只限制第一个Error
		if err := errlist.First(); err != nil {
			log.Infof("Session [%p] closed, Error = %v", s, err)
		} else {
			log.Infof("Session [%p] closed, Quit", s)
		}
	}()

	// 来自connection的各种请求
	tasks := make(chan *Request, maxPipeline)
	go func() {
		defer func() {
			// 出现错误了，直接关闭Session
			s.Close()
			for _ = range tasks { // close(tasks)关闭for loop
			}
		}()
		if err := s.loopWriter(tasks); err != nil {
			errlist.PushBack(err)
		}
	}()

	var wait sync.WaitGroup
	for true {
		// Reader不停地解码， 将Request
		request, err := s.ReadFrame()

		if err != nil {
			errlist.PushBack(err)
			break
		}

		wait.Add(1)
		go func() {
			// 异步执行
			r, _ := s.handleRequest(request, d)

			// 数据请求完毕之后，将Request交给tasks, 然后再写回Client
			tasks <- r
			wait.Done()
		}()
	}
	// 等待go func执行完毕
	wait.Wait()
	close(tasks)
	return
}

//
//
// NonBlock和Block的区别:
// NonBlock的 Request和Response是不需要配对的， Request和Response是独立的，例如:
//  ---> RequestA, RequestB
//  <--- RequestB, RequestA 后请求的，可以先返回
//
func (s *NonBlockSession) loopWriter(tasks <-chan *Request) error {
	for r := range tasks {
		// 1. tasks中的请求是已经请求完毕的，loopWriter负责将它们的数据写回到rpc proxy
		s.handleResponse(r)

		// 2. 将结果写回给Client
		_, err := s.TBufferedFramedTransport.Write(r.Response.Data)
		if err != nil {
			log.ErrorErrorf(err, "Write back Data Error: %v\n", err)
			return err
		}

		// 3. Flush
		err = s.TBufferedFramedTransport.FlushBuffer(len(tasks) == 0) // len(tasks) == 0
		if err != nil {
			return err
		}
	}
	return nil
}

// 获取可以直接返回给Client的response
func (s *NonBlockSession) handleResponse(r *Request) {

	// 将Err转换成为Exception
	if r.Response.Err != nil {
		log.Println("#handleResponse, Error ----> Reponse Data")
		r.Response.Data = GetThriftException(r, "nonblock_session")
	}

	incrOpStats(r.Request.Name, microseconds()-r.Start)
}

// 处理来自Client的请求
// 将它的请教交给后端的Dispatcher
//
func (s *NonBlockSession) handleRequest(request []byte, d Dispatcher) (*Request, error) {
	// 构建Request
	//	log.Printf("HandleRequest: %s\n", string(request))
	// 来自proxy的请求, request中不带有service
	r := NewRequest(request, false)

	// 处理心跳
	if r.Request.TypeId == MESSAGE_TYPE_HEART_BEAT {
		//		log.Printf(Magenta("PING/PANG"))
		HandlePingRequest(r)
		return r, nil
	}

	// 正常请求
	if s.lastRequestTime != nil {
		s.lastRequestTime.Set(time.Now().Unix())
	}
	// 增加统计
	s.LastOpUnix = time.Now().Unix()
	s.Ops++

	// 交给Dispatch
	return r, d.Dispatch(r)
}
