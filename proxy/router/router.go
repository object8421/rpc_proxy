//// Copyright 2014 Wandoujia Inc. All Rights Reserved.
//// Licensed under the MIT (MIT-LICENSE.txt) license.

package router

//import (
//	"strings"
//	"sync"

//	"git.chunyu.me/infra/rpc_proxy/utils/errors"
//	"git.chunyu.me/infra/rpc_proxy/utils/log"
//)

//const MaxSlotNum = models.DEFAULT_SLOT_NUM

//type Router struct {
//	mu   sync.Mutex
//	pool map[string]*SharedBackendConn

//	//	slots [MaxSlotNum]*Slot

//	closed bool
//}

//func New() *Router {
//	s := &Router{
//		pool: make(map[string]*SharedBackendConn),
//	}
//	return s
//}

//func (s *Router) Close() error {
//	//	s.mu.Lock()
//	//	defer s.mu.Unlock()
//	//	if s.closed {
//	//		return nil
//	//	}
//	//	for i := 0; i < len(s.slots); i++ {
//	//		s.resetSlot(i)
//	//	}
//	//	s.closed = true
//	//	return nil
//}

//var errClosedRouter = errors.New("use of closed router")

//func (s *Router) KeepAlive() error {
//	s.mu.Lock()
//	defer s.mu.Unlock()
//	if s.closed {
//		return errClosedRouter
//	}
//	for _, bc := range s.pool {
//		bc.KeepAlive()
//	}
//	return nil
//}

//// 后端如何处理一个Request?
//func (s *Router) Dispatch(r *Request) error {
//	//	// 获取slot
//	//	hkey := getHashKey(r.Resp, r.OpStr)
//	//	slot := s.slots[hashSlot(hkey)]

//	//	return slot.forward(r, hkey)
//	return nil
//}

//func (s *Router) getBackendConn(addr string) *SharedBackendConn {
//	bc := s.pool[addr]
//	if bc != nil {
//		bc.IncrRefcnt()
//	} else {
//		bc = NewSharedBackendConn(addr, s.auth)
//		s.pool[addr] = bc
//	}
//	return bc
//}

//func (s *Router) putBackendConn(bc *SharedBackendConn) {
//	if bc != nil && bc.Close() {
//		delete(s.pool, bc.Addr())
//	}
//}
