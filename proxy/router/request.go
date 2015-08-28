// Copyright 2014 Wandoujia Inc. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package router

import (
	thrift "git.apache.org/thrift.git/lib/go/thrift"
	"git.chunyu.me/infra/rpc_proxy/utils/atomic2"
	"sync"
)

type Dispatcher interface {
	Dispatch(r *Request) error
}

type Request struct {
	Service string // 服务

	Request struct { // 原始的数据(虽然拷贝有点点效率低，但是和zeromq相比也差不多)
		Name   string
		TypeId thrift.TMessageType
		SeqId  int32
		Data   []byte
	}

	OpStr string
	Start int64

	// 原始请求的内容

	Coalesce func() error

	// 返回的数据类型
	Response struct {
		Data  []byte
		Err   error
		SeqId int32 // -1保留，表示没有对应的SeqNum
	}

	Wait *sync.WaitGroup
	//	slot *sync.WaitGroup

	Failed *atomic2.Bool
}

func DecodeSeqId(data []byte) (seqId int32, err error) {
	transport := NewTMemoryBufferWithBuf(data)
	protocol := thrift.NewTBinaryProtocolTransport(transport)

	_, _, seqId, err = protocol.ReadMessageBegin()
	return
}

//
// 将Request中的SeqNum进行替换
//
func (r *Request) ReplaceSeqId(newSeq int32) {
	if r.Request.Data != nil {
		transport := NewTMemoryBufferWithBuf(r.Request.Data)
		protocol := thrift.NewTBinaryProtocolTransport(transport)

		r.Request.Name, r.Request.TypeId, r.Request.SeqId, _ = protocol.ReadMessageBegin()
		r.Response.SeqId = newSeq

		transport.Close()
		protocol.WriteMessageBegin(r.Request.Name, r.Request.TypeId, newSeq)
	}

}

func (r *Request) RestoreSeqId() {
	if r.Response.Data != nil {
		transport := NewTMemoryBufferWithBuf(r.Response.Data)
		protocol := thrift.NewTBinaryProtocolTransport(transport)

		// 切换回原始的SeqId
		protocol.WriteMessageBegin(r.Request.Name, r.Request.TypeId, r.Request.SeqId)
	}
}
