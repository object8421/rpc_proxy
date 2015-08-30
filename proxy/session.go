// Copyright 2014 Wandoujia Inc. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"encoding/json"
	thrift "git.apache.org/thrift.git/lib/go/thrift"
	"git.chunyu.me/infra/rpc_proxy/utils/atomic2"
	"git.chunyu.me/infra/rpc_proxy/utils/errors"
	"git.chunyu.me/infra/rpc_proxy/utils/log"
	"time"
)

//
// 用于rpc proxy或者load balance用来管理Client的
//
type Session struct {
	*TBufferedFramedTransport

	RemoteAddress string
	Ops           int64
	LastOpUnix    int64
	CreateUnix    int64

	quit    bool
	failed  atomic2.Bool
	closed  atomic2.Bool
	verbose bool
}

// 返回当前Session的状态
func (s *Session) String() string {
	o := &struct {
		Ops        int64  `json:"ops"`
		LastOpUnix int64  `json:"lastop"`
		CreateUnix int64  `json:"create"`
		RemoteAddr string `json:"remote"`
	}{
		s.Ops, s.LastOpUnix, s.CreateUnix,
		s.RemoteAddress,
	}
	b, _ := json.Marshal(o)
	return string(b)
}

// c： client <---> proxy之间的连接
func NewSession(c thrift.TTransport, address string, verbose bool) *Session {
	return NewSessionSize(c, address, verbose, 1024*32, 1800)
}

func NewSessionSize(c thrift.TTransport, address string, verbose bool, bufsize int, timeout int) *Session {
	s := &Session{
		CreateUnix:               time.Now().Unix(),
		RemoteAddress:            address,
		verbose:                  verbose,
		TBufferedFramedTransport: NewTBufferedFramedTransport(c, time.Microsecond*100, 20),
	}

	// Reader 处理Client发送过来的消息
	// Writer 将后端服务的数据返回给Client
	log.Infof(Green("NewSession To: %s\n"), s.RemoteAddress)
	return s
}

func (s *Session) Close() error {
	s.failed.Set(true)
	s.closed.Set(true)
	return s.TBufferedFramedTransport.Close()
}

func (s *Session) IsClosed() bool {
	return s.closed.Get()
}

func (s *Session) Serve(d Dispatcher, maxPipeline int) {
	var errlist errors.ErrorList
	defer func() {
		log.Printf(Red("Session Over: %s, Print Error List: %d errors\n"), s.RemoteAddress, errlist.Len())

		if err := errlist.First(); err != nil {
			log.Infof("session [%p] closed, error = %s", s, s, err)
		} else {
			log.Infof("session [%p] closed, quit", s, s)
		}
	}()

	// 来自connection的各种请求
	tasks := make(chan *Request, maxPipeline)
	go func() {
		defer func() {
			// 出现错误了，直接关闭Session
			s.Close()
			for _ = range tasks {
			}
		}()
		if err := s.loopWriter(tasks); err != nil {
			errlist.PushBack(err)
		}
	}()

	defer close(tasks)

	// 从Client读取用户的请求，然后再交给Dispatcher来处理
	if err := s.loopReader(tasks, d); err != nil {
		errlist.PushBack(err)
	}
	log.Info(Cyan("loopReader Over, Session#Serve Over"))
}

// 从Client读取数据
func (s *Session) loopReader(tasks chan<- *Request, d Dispatcher) error {
	if d == nil {
		return errors.New("nil dispatcher")
	}
	for !s.quit {
		// client <--> rpc
		// 从client读取frames
		request, err := s.ReadFrame()
		if err != nil {
			// 遇到EOF等错误，就直接结束loopReader
			// 结束之前需要和后端的back_conn之间处理好关系?
			log.ErrorErrorf(err, Red("ReadFrame Error: %v\n"), err)
			return err
		}

		r, err := s.handleRequest(request, d)
		if err != nil {
			return err
		} else {
			if s.verbose {
				log.Info("Succeed Get Result")
			}

			// 将请求交给: tasks, 同一个Session中的请求是
			tasks <- r
		}
	}
	return nil
}

func (s *Session) loopWriter(tasks <-chan *Request) error {
	// Proxy: Session ---> Client
	for r := range tasks {
		// 1. 等待Request对应的Response
		//    出错了如何处理呢?
		s.handleResponse(r)

		// 2. 将结果写回给Client
		if s.verbose {
			log.Printf("Session#loopWriter --> client[%d]: %s\n", len(r.Response.Data), Cyan(string(r.Response.Data)))

			r1 := NewRequest(r.Response.Data)
			log.Printf("====> Service: %s, Name: %s, Seq: %d, Type: %d\n", r1.Service, r1.Request.Name, r1.Request.SeqId, r1.Request.TypeId)
		}

		// r.Response.Data ---> Client
		_, err := s.TBufferedFramedTransport.Write(r.Response.Data)
		if err != nil {
			log.ErrorErrorf(err, "Write back Data Error: %v\n", err)
			return err
		}

		// 3. Flush
		err = s.TBufferedFramedTransport.FlushBuffer(true) // len(tasks) == 0
		if err != nil {
			log.ErrorErrorf(err, "Write back Data Error: %v\n", err)
			return err
		}
	}
	return nil
}

//
//
// 等待Request请求的返回: Session最终被Block住
//
func (s *Session) handleResponse(r *Request) {
	// 等待结果的出现
	r.Wait.Wait()

	// 将Err转换成为Exception
	if r.Response.Err != nil {

		r.Response.Data = GetThriftException(r)
		log.Printf(Magenta("---->Convert Error Back to Exception: %s\n"), string(r.Response.Data))
	}

	// 如何处理Data和Err呢?
	incrOpStats(r.OpStr, microseconds()-r.Start)
}

// 处理来自Client的请求
func (s *Session) handleRequest(request []byte, d Dispatcher) (*Request, error) {
	// 构建Request
	if s.verbose {
		log.Printf("HandleRequest: %s\n", string(request))
	}
	r := NewRequest(request)

	// 增加统计
	s.LastOpUnix = time.Now().Unix()
	s.Ops++

	// 交给Dispatch
	return r, d.Dispatch(r)
}

func (s *Session) handleQuit(r *Request) (*Request, error) {
	s.quit = true
	//	r.Response.Resp = redis.NewString([]byte("OK"))
	return r, nil
}

func (s *Session) handlePing(r *Request) (*Request, error) {
	//	if len(r.Resp.Array) != 1 {
	//		r.Response.Resp = redis.NewError([]byte("ERR wrong number of arguments for 'PING' command"))
	//		return r, nil
	//	}
	//	r.Response.Resp = redis.NewString([]byte("PONG"))
	//	return r, nil
	return nil, nil
}

func microseconds() int64 {
	return time.Now().UnixNano() / int64(time.Microsecond)
}
