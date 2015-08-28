package proxy

//import (
//	thrift "git.apache.org/thrift.git/lib/go/thrift"
//	"git.chunyu.me/infra/rpc_proxy/utils/assert"
//	"net"
//	"strconv"
//	"sync"
//	"testing"
//	"time"
//)

//func TestBackend(t *testing.T) {
//	l, err := net.Listen("tcp", "127.0.0.1:0")
//	assert.MustNoError(err)
//	defer l.Close()

//	addr := l.Addr().String()
//	reqc := make(chan *Request, 10)
//	go func() {
//		// 创建一个BackendConn
//		bc := NewBackendConn(addr, nil)
//		defer bc.Close()
//		defer close(reqc)

//		for i := 0; i < cap(reqc); i++ {

//			data := make([]byte, 1000, 1000)
//			size := fakeData("hello", thrift.CALL, 0, data[0:0])
//			data = data[0:size]

//			r := NewRequest(data)

//			bc.PushBack(r)
//			reqc <- r
//		}
//	}()

//	go func() {
//		c, err := l.Accept()
//		assert.MustNoError(err)
//		defer c.Close()
//		conn := redis.NewConn(c)
//		time.Sleep(time.Millisecond * 300)
//		for i := 0; i < cap(reqc); i++ {
//			_, err := conn.Reader.Decode()
//			assert.MustNoError(err)
//			resp := redis.NewString([]byte(strconv.Itoa(i)))
//			assert.MustNoError(conn.Writer.Encode(resp, true))
//		}
//	}()

//	var n int
//	for r := range reqc {
//		r.Wait.Wait()
//		assert.Must(string(r.Response.Resp.Value) == strconv.Itoa(n))
//		n++
//	}
//	assert.Must(n == cap(reqc))
//}
