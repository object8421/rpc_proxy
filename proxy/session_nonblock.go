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
type NonBlockSession struct {
	*TBufferedFramedTransport

	RemoteAddress string
	Ops           int64
	LastOpUnix    int64
	CreateUnix    int64

	quit   bool
	failed atomic2.Bool
	closed atomic2.Bool
}

// 返回当前Session的状态
func (s *NonBlockSession) String() string {
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

func NewNonBlockSession(c thrift.TTransport, address string) *NonBlockSession {
	return NewNonBlockSessionSize(c, address, 1024*32, 1800)
}

func NewNonBlockSessionSize(c thrift.TTransport, address string, bufsize int, timeout int) *NonBlockSession {
	s := &NonBlockSession{
		CreateUnix:               time.Now().Unix(),
		RemoteAddress:            address,
		TBufferedFramedTransport: NewTBufferedFramedTransport(c, time.Microsecond*200, 20),
	}

	// 还是基于c net.Conn进行读写，只是采用Redis协议进行编码解码
	// Reader 处理Client发送过来的消息
	// Writer 将后端服务的数据返回给Client
	log.Infof("session [%p] create: %s", s, s)
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
		if err := errlist.First(); err != nil {
			log.Infof("session [%p] closed: %s, error = %s", s, s, err)
		} else {
			log.Infof("session [%p] closed: %s, quit", s, s)
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

	//	// 从Client读取用户的请求，然后再交给Dispatcher来处理
	//	if err := s.loopReader(tasks, d); err != nil {
	//		errlist.PushBack(err)
	//	}
	//}

	//// 从Client读取数据
	//func (s *Session) loopReader(tasks chan<- *Request, d Dispatcher) error {
	//	if d == nil {
	//		return errors.New("nil dispatcher")
	//	}
	for !s.quit {
		// Reader不停地解码， 将Request
		request, err := s.ReadFrame()
		if err != nil {
			errlist.PushBack(err)
			return
		}

		go func() {
			// 异步执行
			r, _ := s.handleRequest(request, d)
			log.Info("Succeed Get Result")
			tasks <- r
		}()
	}
	return
}

func (s *NonBlockSession) loopWriter(tasks <-chan *Request) error {
	for r := range tasks {
		// 1. 等待Request对应的Response
		//    出错了如何处理呢?
		_, err := s.handleResponse(r)
		if err != nil {
			// TODO: 如果不是Client的问题，服务器最好通知Client发生什么问题了
			return err
		}

		// 2. 将结果写回给Client
		log.Printf("xxx Session Write back to client: %s, tasks:%d\n", string(r.Response.Data), len(tasks))

		log.Printf("xxx Write Back Frame Length: %d\n", len(r.Response.Data))
		_, err = s.TBufferedFramedTransport.Write(r.Response.Data)
		if err != nil {
			log.ErrorErrorf(err, "Write back Data Error: %v\n", err)
			return err
		}

		// 3. Flush
		err = s.TBufferedFramedTransport.FlushBuffer(true) // len(tasks) == 0
		if err != nil {
			return err
		}
	}
	return nil
}

// 获取可以直接返回给Client的response
func (s *NonBlockSession) handleResponse(r *Request) (resp []byte, e error) {
	// 等待结果的出现
	r.Wait.Wait()

	// 合并?
	if r.Coalesce != nil {
		if err := r.Coalesce(); err != nil {
			return nil, err
		}
	}
	resp, err := r.Response.Data, r.Response.Err
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, ErrRespIsRequired
	}
	incrOpStats(r.OpStr, microseconds()-r.Start)
	return resp, nil
}

// 处理来自Client的请求
func (s *NonBlockSession) handleRequest(request []byte, d Dispatcher) (*Request, error) {
	// 构建Request
	log.Printf("HandleRequest: %s\n", string(request))
	r := NewRequest(request)

	// 增加统计
	s.LastOpUnix = time.Now().Unix()
	s.Ops++

	// 交给Dispatch
	return r, d.Dispatch(r)
}

func (s *NonBlockSession) handleQuit(r *Request) (*Request, error) {
	s.quit = true
	//	r.Response.Resp = redis.NewString([]byte("OK"))
	return r, nil
}

func (s *NonBlockSession) handlePing(r *Request) (*Request, error) {
	//	if len(r.Resp.Array) != 1 {
	//		r.Response.Resp = redis.NewError([]byte("ERR wrong number of arguments for 'PING' command"))
	//		return r, nil
	//	}
	//	r.Response.Resp = redis.NewString([]byte("PONG"))
	//	return r, nil
	return nil, nil
}
