//// Copyright 2015 Spring Rain Software Compnay LTD. All Rights Reserved.
//// Licensed under the MIT (MIT-LICENSE.txt) license.
package proxy

import (
	thrift "git.apache.org/thrift.git/lib/go/thrift"
	"time"
)

const (
	HB_TIMEOUT = 6
)

//
// 生成Thrift格式的Exception Message
//
func HandlePingRequest(req *Request) {
	req.Response.Data = req.Request.Data
	// 构建thrift的Transport
	//	transport := thrift.NewTMemoryBufferLen(1024)
	//	protocol := thrift.NewTBinaryProtocolTransport(transport)
	//	protocol.WriteMessageBegin(req.Request.Name, MESSAGE_TYPE_HEART_BEAT, req.Request.SeqId)
	//	protocol.WriteMessageEnd()
	//	protocol.Flush()

	//	bytes := transport.Bytes()
	//	return bytes
}

func NewPingRequest(seqId int32) *Request {
	// 构建thrift的Transport
	transport := thrift.NewTMemoryBufferLen(1024)
	protocol := thrift.NewTBinaryProtocolTransport(transport)
	protocol.WriteMessageBegin("ping", MESSAGE_TYPE_HEART_BEAT, seqId)
	protocol.WriteMessageEnd()
	protocol.Flush()

	r := &Request{}
	r.ServiceInRequest = false
	r.Start = time.Now().Unix()
	r.Request.Data = transport.Bytes()
	r.Request.Name = "ping"
	r.Request.SeqId = seqId
	r.Request.TypeId = MESSAGE_TYPE_HEART_BEAT
	return r
}
