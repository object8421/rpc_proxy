package proxy

const (
	// Thrift协议中 SEQ_ID的访问
	BACKEND_CONN_MIN_SEQ_ID    = 1
	BACKEND_CONN_MAX_SEQ_ID    = 1000000
	INVALID_ARRAY_INDEX        = -1          // 无效的数组元素下标
	HB_TIMEOUT                 = 6           // 心跳超时时间间隔
	REQUEST_EXPIRED_TIME_MICRO = 25 * 100000 // 2.5s
	TEST_PRODUCT_NAME          = "test"
	VERSION                    = "0.1.0-2015090621" // 版本信息
)
