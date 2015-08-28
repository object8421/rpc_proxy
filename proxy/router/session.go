//// Copyright 2014 Wandoujia Inc. All Rights Reserved.
//// Licensed under the MIT (MIT-LICENSE.txt) license.

package router

//import (
//	"encoding/json"

//	"net"
//	"sync"
//	"time"

//	"git.chunyu.me/infra/rpc_proxy/utils/atomic2"
//	"git.chunyu.me/infra/rpc_proxy/utils/errors"
//	"git.chunyu.me/infra/rpc_proxy/utils/log"
//)

////
//// 用于rpc proxy或者load balance用来管理Client的
////
//type Session struct {
//	//	*redis.Conn

//	Ops int64

//	LastOpUnix int64
//	CreateUnix int64

//	quit   bool
//	failed atomic2.Bool
//	closed atomic2.Bool
//}

//// 返回当前Session的状态
//func (s *Session) String() string {
//	o := &struct {
//		Ops        int64  `json:"ops"`
//		LastOpUnix int64  `json:"lastop"`
//		CreateUnix int64  `json:"create"`
//		RemoteAddr string `json:"remote"`
//	}{
//		s.Ops, s.LastOpUnix, s.CreateUnix,
//		s.Conn.Sock.RemoteAddr().String(),
//	}
//	b, _ := json.Marshal(o)
//	return string(b)
//}

//func NewSession(c net.Conn) *Session {
//	return NewSessionSize(c, 1024*32, 1800)
//}

//func NewSessionSize(c net.Conn, bufsize int, timeout int) *Session {
//	s := &Session{CreateUnix: time.Now().Unix()}

//	// 还是基于c net.Conn进行读写，只是采用Redis协议进行编码解码
//	// Reader 处理Client发送过来的消息
//	// Writer 将后端服务的数据返回给Client
//	s.Conn = redis.NewConnSize(c, bufsize)

//	s.Conn.ReaderTimeout = time.Second * time.Duration(timeout)
//	s.Conn.WriterTimeout = time.Second * 30
//	log.Infof("session [%p] create: %s", s, s)
//	return s
//}

//func (s *Session) Close() error {
//	s.failed.Set(true)
//	s.closed.Set(true)
//	return s.Conn.Close()
//}

//func (s *Session) IsClosed() bool {
//	return s.closed.Get()
//}

//func (s *Session) Serve(d Dispatcher, maxPipeline int) {
//	var errlist errors.ErrorList
//	defer func() {
//		if err := errlist.First(); err != nil {
//			log.Infof("session [%p] closed: %s, error = %s", s, s, err)
//		} else {
//			log.Infof("session [%p] closed: %s, quit", s, s)
//		}
//	}()

//	// 来自connection的各种请求
//	tasks := make(chan *Request, maxPipeline)
//	go func() {
//		defer func() {
//			s.Close()
//			for _ = range tasks {
//			}
//		}()
//		if err := s.loopWriter(tasks); err != nil {
//			errlist.PushBack(err)
//		}
//	}()

//	defer close(tasks)
//	if err := s.loopReader(tasks, d); err != nil {
//		errlist.PushBack(err)
//	}
//}

//// 从redis读取数据
//func (s *Session) loopReader(tasks chan<- *Request, d Dispatcher) error {
//	if d == nil {
//		return errors.New("nil dispatcher")
//	}
//	for !s.quit {
//		// Reader不停地解码， 将Request
//		resp, err := s.Reader.Decode()
//		if err != nil {
//			return err
//		}
//		r, err := s.handleRequest(resp, d)
//		if err != nil {
//			return err
//		} else {
//			tasks <- r
//		}
//	}
//	return nil
//}

//func (s *Session) loopWriter(tasks <-chan *Request) error {
//	p := &FlushPolicy{
//		Encoder:     s.Writer,
//		MaxBuffered: 32,
//		MaxInterval: 300,
//	}
//	for r := range tasks {
//		resp, err := s.handleResponse(r)
//		if err != nil {
//			return err
//		}
//		// 将resp写入redis-conn中
//		if err := p.Encode(resp, len(tasks) == 0); err != nil {
//			return err
//		}
//	}
//	return nil
//}

//var ErrRespIsRequired = errors.New("resp is required")

////func (s *Session) handleResponse(r *Request) (*redis.Resp, error) {
////	r.Wait.Wait()
////	if r.Coalesce != nil {
////		if err := r.Coalesce(); err != nil {
////			return nil, err
////		}
////	}
////	resp, err := r.Response.Resp, r.Response.Err
////	if err != nil {
////		return nil, err
////	}
////	if resp == nil {
////		return nil, ErrRespIsRequired
////	}
////	incrOpStats(r.OpStr, microseconds()-r.Start)
////	return resp, nil
////}

////// 处理来自Client的请求
////func (s *Session) handleRequest(resp *redis.Resp, d Dispatcher) (*Request, error) {

////	// 读取Service

////	// 创建Request

////	// 通过Dispatch来处理Request

////	// 完事
////	usnow := microseconds()
////	s.LastOpUnix = usnow / 1e6
////	s.Ops++

////	r := &Request{
////		OpStr:  opstr,
////		Start:  usnow,
////		Resp:   resp,
////		Wait:   &sync.WaitGroup{},
////		Failed: &s.failed,
////	}

////	//	switch opstr {
////	//	case "SELECT":
////	//		return s.handleSelect(r)
////	//	case "PING":
////	//		return s.handlePing(r)
////	//	case "MGET":
////	//		return s.handleRequestMGet(r, d)
////	//	case "MSET":
////	//		return s.handleRequestMSet(r, d)
////	//	case "DEL":
////	//		return s.handleRequestMDel(r, d)
////	//	}
////	return r, d.Dispatch(r)
////}

//func (s *Session) handleQuit(r *Request) (*Request, error) {
//	s.quit = true
//	//	r.Response.Resp = redis.NewString([]byte("OK"))
//	return r, nil
//}

//func (s *Session) handlePing(r *Request) (*Request, error) {
//	//	if len(r.Resp.Array) != 1 {
//	//		r.Response.Resp = redis.NewError([]byte("ERR wrong number of arguments for 'PING' command"))
//	//		return r, nil
//	//	}
//	//	r.Response.Resp = redis.NewString([]byte("PONG"))
//	//	return r, nil
//	return nil, nil
//}

//func microseconds() int64 {
//	return time.Now().UnixNano() / int64(time.Microsecond)
//}
