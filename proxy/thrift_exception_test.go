package proxy

import (
	"fmt"
	thrift "git.apache.org/thrift.git/lib/go/thrift"
	"git.chunyu.me/infra/rpc_proxy/utils/assert"
	"strings"
	"testing"
)

//
// go test git.chunyu.me/infra/rpc_proxy/proxy -v -run "TestGetThriftException"
//
func TestGetThriftException(t *testing.T) {

	serviceName := "accounts"
	data := GetServiceNotFoundData(serviceName, 0)
	fmt.Println("Exception Data: ", string(data))

	transport := NewTMemoryBufferWithBuf(data)
	exc := thrift.NewTApplicationException(-1, "")
	protocol := thrift.NewTBinaryProtocolTransport(transport)

	// 注意: Read函数返回的是一个新的对象
	protocol.ReadMessageBegin()
	exc, _ = exc.Read(protocol)
	protocol.ReadMessageEnd()

	fmt.Println("Exc: ", exc.TypeId(), "Error: ", exc.Error())

	var errMsg string = exc.Error()
	assert.Must(strings.Contains(errMsg, serviceName))
}
