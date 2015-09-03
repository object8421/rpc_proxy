//// Copyright 2015 Spring Rain Software Compnay LTD. All Rights Reserved.
//// Licensed under the MIT (MIT-LICENSE.txt) license.
package proxy

import (
	"os"
	//	"fmt"
	//	thrift "git.apache.org/thrift.git/lib/go/thrift"
	"git.chunyu.me/infra/rpc_proxy/utils/log"
	"github.com/stretchr/testify/assert"
	//	"io"
	"testing"
)

func init() {
	log.SetLevel(log.LEVEL_INFO)
	log.SetFlags(log.Flags() | log.Lshortfile)
}

//
// go test git.chunyu.me/infra/rpc_proxy/proxy -v -run "TestSocketPermissionChange"
//
func TestSocketPermissionChange(t *testing.T) {

	socketFile := "aaa.sock"
	if FileExist(socketFile) {
		os.Remove(socketFile)
	}
	s, _ := NewTServerUnixDomain(socketFile)
	s.Listen()

	assert.True(t, true)

}
