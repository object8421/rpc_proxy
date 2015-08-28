// Copyright 2014 Wandoujia Inc. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package router

import (
	"fmt"
	"sync"
	"time"

	thrift "git.apache.org/thrift.git/lib/go/thrift"
	"git.chunyu.me/infra/rpc_proxy/utils/errors"
	"git.chunyu.me/infra/rpc_proxy/utils/log"
)

type BackendConn struct {
	addr string
	stop sync.Once

	input chan *Request // 输入的请求, 有: 1024个Buffer

	// seqNum2Request 读写基本上差不多
	sync.Mutex
	seqNum2Request map[int32]*Request
	currentSeqNum  int32 // 范围: 1 ~ 100000
}

func NewBackendConn(addr string) *BackendConn {
	bc := &BackendConn{
		addr:           addr,
		input:          make(chan *Request, 1024),
		seqNum2Request: make(map[int32]*Request, 4096),
		currentSeqNum:  1,
	}
	go bc.Run()
	return bc
}

//
// 不断建立到后端的逻辑，负责: BackendConn#input到redis的数据的输入和返回
//
func (bc *BackendConn) Run() {
	log.Infof("backend conn [%p] to %s, start service", bc, bc.addr)
	for k := 0; ; k++ {
		// 1. 首先BackendConn将当前 input中的数据写到后端服务中
		err := bc.loopWriter()

		if err == nil {
			break
		} else {
			// 如果出现err, 则将bc.input中现有的数据都flush回去（直接报错)
			for i := len(bc.input); i != 0; i-- {
				r := <-bc.input
				bc.setResponse(r, nil, err)
			}
		}

		// 然后等待，继续尝试新的连接
		log.WarnErrorf(err, "backend conn [%p] to %s, restart [%d]", bc, bc.addr, k)
		time.Sleep(time.Millisecond * 50)
	}
	log.Infof("backend conn [%p] to %s, stop and exit", bc, bc.addr)
}

func (bc *BackendConn) Addr() string {
	return bc.addr
}

func (bc *BackendConn) Close() {
	bc.stop.Do(func() {
		close(bc.input)
	})
}

func (bc *BackendConn) PushBack(r *Request) {
	if r.Wait != nil {
		r.Wait.Add(1)
	}
	bc.input <- r
}

func (bc *BackendConn) KeepAlive() bool {
	return false
	//	if len(bc.input) != 0 {
	//		return false
	//	}
	//	r := &Request{
	//		Resp: redis.NewArray([]*redis.Resp{
	//			redis.NewBulkBytes([]byte("PING")),
	//		}),
	//	}

	//	select {
	//	case bc.input <- r:
	//		return true
	//	default:
	//		return false
	//	}
}

var ErrFailedRequest = errors.New("discard failed request")

// Codis的BackendConn如何实现呢?
func (bc *BackendConn) loopWriter() error {
	// bc.input 实现了前段请求的buffer
	r, ok := <-bc.input

	var c *TBufferedFramedTransport
	var err error

	if ok {
		c, err = bc.newBackendReader()

		// 如果出错，则表示连接Redis或授权出问题了
		if err != nil {
			return bc.setResponse(r, nil, err)
		}

		for ok {
			// 如果暂时没有数据输入，则p策略可能就有问题了
			// 只有写入数据，才有可能产生flush; 如果是最后一个数据必须自己flush, 否则就可能无限期等待
			//
			var flush = len(bc.input) == 0

			fmt.Printf("%d\n", flush)
			if bc.canForward(r) {
				r.ReplaceSeqId(bc.currentSeqNum)

				// 主动控制Buffer的flush
				c.Write(r.Request.Data)
				c.FlushBuffer(flush)

				// 备案(只有loopWriter操作，不加锁)
				bc.currentSeqNum++
				if bc.currentSeqNum > 100000 {
					bc.currentSeqNum = 1
				}

				bc.Lock()
				bc.seqNum2Request[r.Response.SeqId] = r
				bc.Unlock()

			} else {
				// 如果bc不能写入数据，则直接

				if err := c.FlushBuffer(flush); err != nil {
					return bc.setResponse(r, nil, err)
				}

				// 请求压根就没有发送
				bc.setResponse(r, nil, ErrFailedRequest)
			}

			// 继续读取请求, 如果有异常，如何处理呢?
			r, ok = <-bc.input
		}
	}
	return nil
}

// 创建一个到"后端服务"的连接
func (bc *BackendConn) newBackendReader() (*TBufferedFramedTransport, error) {

	// 创建连接
	socket, err := thrift.NewTSocketTimeout(bc.addr, time.Second*3)

	if err != nil {
		// 连接不上，失败
		return nil, err
	}

	c := NewTBufferedFramedTransport(socket, 300*time.Microsecond, 64)

	go func() {
		defer c.Close()

		for true {
			resp, err := c.ReadFrame()
			if err != nil {
				bc.flushRequests(err)
				break
			} else {
				bc.setResponse(nil, resp, err)
			}
		}
	}()
	return c, nil
}

// 处理所有的等待中的请求
func (b *BackendConn) flushRequests(err error) {

}

func (bc *BackendConn) canForward(r *Request) bool {
	if r.Failed != nil && r.Failed.Get() {
		return false
	} else {
		return true
	}
}

// 配对 Request, resp, err
// PARAM: resp []byte 为一帧完整的thrift数据包
func (bc *BackendConn) setResponse(r *Request, data []byte, err error) error {
	// 表示出现错误了
	if data == nil {
		r.Response.Err = err
	} else {
		// 从resp中读取基本的信息
		seqId, err := DecodeSeqId(data)

		// 解码错误，直接报错
		if err != nil {
			return err
		}

		// 找到对应的Request
		bc.Lock()
		req, ok := bc.seqNum2Request[seqId]
		if ok {
			delete(bc.seqNum2Request, seqId)
		}
		bc.Unlock()

		if !ok {
			return errors.New("Invalid Response")
		}
		r = req
	}

	r.Response.Data, r.Response.Err = data, err

	// 设置几个控制用的channel
	if err != nil && r.Failed != nil {
		r.Failed.Set(true)
	}
	if r.Wait != nil {
		r.Wait.Done()
	}
	//	if r.slot != nil {
	//		r.slot.Done()
	//	}
	return err
}

//
// 通过引用计数管理后端的BackendConn
//
type SharedBackendConn struct {
	*BackendConn
	mu sync.Mutex

	refcnt int
}

func NewSharedBackendConn(addr string) *SharedBackendConn {
	return &SharedBackendConn{BackendConn: NewBackendConn(addr), refcnt: 1}
}

func (s *SharedBackendConn) Close() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.refcnt <= 0 {
		log.Panicf("shared backend conn has been closed, close too many times")
	}
	if s.refcnt == 1 {
		s.BackendConn.Close()
	}
	s.refcnt--
	return s.refcnt == 0
}

// Close之后不能再引用
// socket不能多次打开，必须重建
//
func (s *SharedBackendConn) IncrRefcnt() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.refcnt == 0 {
		log.Panicf("shared backend conn has been closed")
	}
	s.refcnt++
}
