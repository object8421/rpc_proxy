package proxy

import (
	"fmt"
	thrift "git.apache.org/thrift.git/lib/go/thrift"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

//
// go test git.chunyu.me/infra/rpc_proxy/proxy -v -run "TestBackend"
//
func TestBackend(t *testing.T) {

	// 作为一个Server
	transport, err := thrift.NewTServerSocket("127.0.0.1:0")
	assert.NoError(t, err)
	defer transport.Close()

	err = transport.Listen()
	assert.NoError(t, err)

	addr := transport.Addr().String()

	fmt.Println("Addr: ", addr)

	var requestNum int32 = 10

	requests := make([]*Request, 0, requestNum)

	var i int32
	for i = 0; i < requestNum; i++ {
		buf := make([]byte, 100, 100)
		l := fakeData("Hello", thrift.CALL, i, buf[0:0])
		buf = buf[0:l]

		req := NewRequest(buf)

		requests = append(requests, req)
	}

	go func() {
		tran, err := transport.Accept()
		assert.NoError(t, err)

		bt := NewTBufferedFramedTransport(tran, time.Microsecond*100, 2)

		// 在当前的这个t上读写数据
		var i int32
		for i = 0; i < requestNum; i++ {
			request, err := bt.ReadFrame()
			assert.NoError(t, err)

			// 回写数据
			bt.Write(request)
			bt.FlushBuffer(true)

			req := NewRequest(request)

			assert.Equal(t, req.Request.SeqId, i)
		}

		tran.Close()
	}()

	go func() {
		bc := NewBackendConn(addr, nil)
		defer bc.Close()

		// 准备发送数据
		var i int32
		for i = 0; i < requestNum; i++ {
			bc.input <- requests[i]
		}

		bc.Run()
	}()

	time.Sleep(time.Second)

	fmt.Println("Requests Len: ", len(requests))
	for _, r := range requests {
		r.Wait.Wait()
		assert.Equal(t, len(r.Request.Data), len(r.Response.Data))
	}
}
