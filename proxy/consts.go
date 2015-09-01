package proxy

const (
	// Thrift协议中 SEQ_ID的访问
	BACKEND_CONN_MIN_SEQ_ID = 1
	BACKEND_CONN_MAX_SEQ_ID = 100000
	INVALID_ARRAY_INDEX     = -1 // 无效的数组元素下标
	HB_TIMEOUT              = 6  // 心跳超时时间间隔
)
