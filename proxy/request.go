package proxy

import (
	thrift "git.apache.org/thrift.git/lib/go/thrift"
	"git.chunyu.me/infra/rpc_proxy/utils/atomic2"
	"git.chunyu.me/infra/rpc_proxy/utils/log"
	"strings"
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

func NewRequest(data []byte) *Request {
	request := &Request{
		Wait: &sync.WaitGroup{},
	}
	request.Request.Data = data
	request.DecodeRequest()

	return request

}
func (r *Request) DecodeRequest() {
	transport := NewTMemoryBufferWithBuf(r.Request.Data)
	protocol := thrift.NewTBinaryProtocolTransport(transport)

	r.Request.Name, r.Request.TypeId, r.Request.SeqId, _ = protocol.ReadMessageBegin()

	// 参考 ： TMultiplexedProtocol
	nameFields := strings.SplitN(r.Request.Name, thrift.MULTIPLEXED_SEPARATOR, 2)
	if len(nameFields) != 2 {
		r.Service = ""
	} else {
		r.Service = nameFields[0]
		r.Request.Name = nameFields[1]
	}
}

//
// 将Request中的SeqNum进行替换
//
func (r *Request) ReplaceSeqId(newSeq int32) {
	if r.Request.Data != nil {
		log.Printf(Green("Replace SeqNum: %d --> %d\n"), r.Request.SeqId, newSeq)
		r.Response.SeqId = newSeq

		start := len(r.Service)
		if start > 0 {
			start += 1 // ":"
			log.Printf("Service: %s, Name: %s\n", r.Service, r.Request.Name)
		}
		transport := NewTMemoryBufferWithBuf(r.Request.Data[start:start])
		protocol := thrift.NewTBinaryProtocolTransport(transport)
		protocol.WriteMessageBegin(r.Request.Name, r.Request.TypeId, newSeq)

		// 将service从name中剥离出去
		r.Request.Data = r.Request.Data[start:len(r.Request.Data)]
		log.Printf(Green("Request Data Frame: %d\n"), len(r.Request.Data))
	}

}

func (r *Request) RestoreSeqId() {
	if r.Response.Data != nil {
		log.Printf("RestoreSeqId SeqNum: %d --> %d\n", r.Response.SeqId, r.Request.SeqId)

		transport := NewTMemoryBufferWithBuf(r.Response.Data[0:0])
		protocol := thrift.NewTBinaryProtocolTransport(transport)

		// 切换回原始的SeqId
		protocol.WriteMessageBegin(r.Request.Name, r.Request.TypeId, r.Request.SeqId)
	}
}

func DecodeSeqId(data []byte) (seqId int32, err error) {
	transport := NewTMemoryBufferWithBuf(data)
	protocol := thrift.NewTBinaryProtocolTransport(transport)

	_, _, seqId, err = protocol.ReadMessageBegin()
	return
}
