package proxy

import (
	"fmt"
	thrift "git.apache.org/thrift.git/lib/go/thrift"
)

//
// 生成Thrift格式的Exception Message
//
func GetServiceNotFoundData(req *Request) []byte {
	req.Response.TypeId = thrift.EXCEPTION

	// 构建thrift的Transport
	transport := thrift.NewTMemoryBufferLen(1024)
	protocol := thrift.NewTBinaryProtocolTransport(transport)

	// 构建一个Message, 写入Exception
	msg := fmt.Sprintf("Service: %s Not Found", req.Service)
	exc := thrift.NewTApplicationException(thrift.UNKNOWN_APPLICATION_EXCEPTION, msg)

	protocol.WriteMessageBegin(req.Request.Name, thrift.EXCEPTION, req.Request.SeqId)
	exc.Write(protocol)
	protocol.WriteMessageEnd()
	protocol.Flush()

	bytes := transport.Bytes()
	return bytes
}

func GetWorkerNotFoundData(req *Request, module string) []byte {
	req.Response.TypeId = thrift.EXCEPTION

	// 构建thrift的Transport
	transport := thrift.NewTMemoryBufferLen(1024)
	protocol := thrift.NewTBinaryProtocolTransport(transport)

	// 构建一个Message, 写入Exception
	msg := fmt.Sprintf("Worker FOR %s#%s.%s Not Found", module, req.Service, req.Request.Name)
	exc := thrift.NewTApplicationException(thrift.INTERNAL_ERROR, msg)

	protocol.WriteMessageBegin(req.Request.Name, thrift.EXCEPTION, req.Request.SeqId)
	exc.Write(protocol)
	protocol.WriteMessageEnd()

	bytes := transport.Bytes()
	return bytes
}

func GetThriftException(req *Request, module string) []byte {
	req.Response.TypeId = thrift.EXCEPTION

	// 构建thrift的Transport
	transport := thrift.NewTMemoryBufferLen(1024)
	protocol := thrift.NewTBinaryProtocolTransport(transport)

	msg := fmt.Sprintf("Module: %s, Service: %s, Method: %s, Error: %v", module, req.Service, req.Request.Name, req.Response.Err)

	// 构建一个Message, 写入Exception
	exc := thrift.NewTApplicationException(thrift.INTERNAL_ERROR, msg)

	protocol.WriteMessageBegin(req.Service, thrift.EXCEPTION, req.Request.SeqId)
	exc.Write(protocol)
	protocol.WriteMessageEnd()

	bytes := transport.Bytes()
	return bytes
}

//
// 解析Thrift数据的Message Header
//
func ParseThriftMsgBegin(msg []byte) (name string, typeId thrift.TMessageType, seqId int32, err error) {
	transport := thrift.NewTMemoryBufferLen(1024)
	transport.Write(msg)
	protocol := thrift.NewTBinaryProtocolTransport(transport)
	name, typeId, seqId, err = protocol.ReadMessageBegin()
	return
}