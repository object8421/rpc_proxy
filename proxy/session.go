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

	Ops int64

	LastOpUnix int64
	CreateUnix int64

	quit   bool
	failed atomic2.Bool
	closed atomic2.Bool
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
		"unknow",
	}
	b, _ := json.Marshal(o)
	return string(b)
}

func NewSession(c *thrift.TSocket) *Session {
	return NewSessionSize(c, 1024*32, 1800)
}

func NewSessionSize(c *thrift.TSocket, bufsize int, timeout int) *Session {
	s := &Session{CreateUnix: time.Now().Unix()}

	// 还是基于c net.Conn进行读写，只是采用Redis协议进行编码解码
	// Reader 处理Client发送过来的消息
	// Writer 将后端服务的数据返回给Client

	s.TBufferedFramedTransport = NewTBufferedFramedTransport(c, time.Microsecond*200, 20)
	log.Infof("session [%p] create: %s", s, s)
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

	// 从Client读取用户的请求，然后再交给Dispatcher来处理
	if err := s.loopReader(tasks, d); err != nil {
		errlist.PushBack(err)
	}
}

// 从redis读取数据
func (s *Session) loopReader(tasks chan<- *Request, d Dispatcher) error {
	if d == nil {
		return errors.New("nil dispatcher")
	}
	for !s.quit {
		// Reader不停地解码， 将Request
		request, err := s.TBufferedFramedTransport.ReadFrame()
		if err != nil {
			return err
		}
		r, err := s.handleRequest(request, d)
		if err != nil {
			return err
		} else {
			tasks <- r
		}
	}
	return nil
}

func (s *Session) loopWriter(tasks <-chan *Request) error {
	for r := range tasks {
		// 1. 等待Request对应的Response
		//    出错了如何处理呢?
		_, err := s.handleResponse(r)
		if err != nil {
			// TODO: 如果不是Client的问题，服务器最好通知Client发生什么问题了
			return err
		}

		// 2. 将结果写回给Client
		_, err = s.TBufferedFramedTransport.Write(r.Response.Data)
		if err != nil {
			return err
		}

		// 3. Flush
		err = s.FlushBuffer(len(tasks) == 0)
		if err != nil {
			return err
		}
	}
	return nil
}

var ErrRespIsRequired = errors.New("resp is required")

// 获取可以直接返回给Client的response
func (s *Session) handleResponse(r *Request) (resp []byte, e error) {
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
func (s *Session) handleRequest(request []byte, d Dispatcher) (*Request, error) {
	// 构建Request
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
