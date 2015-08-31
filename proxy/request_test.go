package proxy

import (
	"fmt"
	thrift "git.apache.org/thrift.git/lib/go/thrift"
	"testing"
)

func fakeData(name string, typeId thrift.TMessageType, seqId int32, buf []byte) int {
	transport := NewTMemoryBufferWithBuf(buf)
	protocol := thrift.NewTBinaryProtocolTransport(transport)

	// 切换回原始的SeqId
	protocol.WriteMessageBegin(name, typeId, seqId)
	return transport.Buffer.Len()

}

//
// go test git.chunyu.me/infra/rpc_proxy/proxy -v -run "TestRequest"
//
func TestRequest(t *testing.T) {
	data := make([]byte, 1000, 1000)
	size := fakeData("demo:hello", thrift.CALL, 0, data[0:0])
	data = data[0:size]

	r := NewRequest(data, true)

	if r.Service != "demo" {
		t.Errorf("Service Name should be: %s", "demo")
	}
	if r.Request.Name != "demo:hello" {
		t.Errorf("Request Name should be: %s", "demo:hello")
	}

	fmt.Printf("Name: %s, SeqId: %d, TypeId: %d\n", r.Request.Name, r.Request.SeqId, r.Request.TypeId)

	var newSeqId int32 = 10
	r.ReplaceSeqId(newSeqId)

	r1 := NewRequest(data, true)
	fmt.Printf("Name: %s, SeqId: %d, TypeId: %d\n", r1.Request.Name, r1.Request.SeqId, r1.Request.TypeId)

	if r1.Request.SeqId != newSeqId {
		t.Errorf("ReplaceSeqId not working\n")
	}

	r.Response.Data = data
	r.RestoreSeqId()

	// 恢复正常
	r1 = NewRequest(data, true)
	fmt.Printf("Name: %s, SeqId: %d, TypeId: %d\n", r1.Request.Name, r1.Request.SeqId, r1.Request.TypeId)
}
