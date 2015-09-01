//// Copyright 2015 Spring Rain Software Compnay LTD. All Rights Reserved.
//// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"reflect"
	"unsafe"
)

var memoryBuffer1024 chan []byte = make(chan []byte, 1000)
var memoryBuffer2048 chan []byte = make(chan []byte, 1000)

func debugBuffer1024Size() int {
	return len(memoryBuffer1024)
}

func debugBuffer2048Size() int {
	return len(memoryBuffer2048)
}

//
// 自己管理内存： 申请
//
// 实现逻辑: make([]byte, size, xxx)
//
func getSlice(initSize int, capacity int) []byte {
	if initSize > capacity {
		panic("Invalid Slice Size")
	}

	var result []byte
	if capacity < 1024 {
		select {
		case result = <-memoryBuffer1024:
		default:
			return make([]byte, initSize, 1024)
		}
	} else if capacity < 2048 {
		select {
		case result = <-memoryBuffer2048:
		default:
			return make([]byte, initSize, 2048)
		}
	} else {
		return make([]byte, initSize, capacity)
	}

	// 将所有的Slice
	return initSlice(result, initSize)
}

func initSlice(s []byte, initSize int) []byte {
	var b []byte

	h := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	h.Data = (*reflect.SliceHeader)(unsafe.Pointer(&s)).Data
	h.Len = initSize
	h.Cap = cap(s)
	return b
}

//
// 自己管理内存：释放
// 1. 同一个slice不要归还多次
// 2. 经过slice处理之后的slice不要归还，例如: v = v[3:4],
//
func returnSlice(slice []byte) bool {
	if cap(slice) == 1024 {
		select {
		case memoryBuffer1024 <- slice:
			return true
		default:
			// DO NOTHING
		}
	} else if cap(slice) == 2048 {
		select {
		case memoryBuffer2048 <- slice:
			return true
		default:
			// DO NOTHING

		}
	}
	return false
}
