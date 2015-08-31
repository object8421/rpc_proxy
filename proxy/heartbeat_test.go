package proxy

import (
	"encoding/binary"
	"fmt"
	"github.com/stretchr/testify/assert"

	"testing"
)

//
// go test git.chunyu.me/infra/rpc_proxy/proxy -v -run "TestHeatbeat"
//
func TestHeatbeat(t *testing.T) {

	request := NewPingRequest(10)

	fmt.Printf("Data Len: %d, Data:[%s]\n", len(request.Request.Data), request.Request.Data)

	for i := 0; i < len(request.Request.Data); i++ {
		fmt.Printf("%d,", request.Request.Data[i])
	}
	fmt.Println()

	assert.False(t, request.ServiceInRequest)

	request.Service = "测试"
	len1 := request.Request.Data
	request.ReplaceSeqId(121)

	len2 := request.Request.Data
	assert.Equal(t, len1, len2)

	buf := make([]byte, 4, 4)
	binary.BigEndian.PutUint32(buf, uint32(16))
	fmt.Println("Frame Len: ")
	for i := 0; i < len(buf); i++ {
		fmt.Printf("%d,", buf[i])
	}
	fmt.Println()

}