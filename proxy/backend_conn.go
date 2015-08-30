package proxy

import (
	"fmt"
	"sync"
	"time"

	thrift "git.apache.org/thrift.git/lib/go/thrift"
	"git.chunyu.me/infra/rpc_proxy/utils/errors"
	"git.chunyu.me/infra/rpc_proxy/utils/log"
)

// 连接的状态
type ConnState int32

const (
	ConnStateInit        ConnState = iota // 初始状态
	ConnStateFailed                       // 连接失败的状态，后续继续尝试连接
	ConnStateActive                       // 激活状态
	ConnStateMarkOffline                  // 标记下线状态(等待所有的请求处理完毕，则下线)
	ConnStateDied                         // 结束
)

type BackendConnStateChanged interface {
	StateChanged(conn *BackendConn)
}

type BackendConn struct {
	addr string
	stop sync.Once

	input    chan *Request // 输入的请求, 有: 1024个Buffer
	readChan chan bool

	// seqNum2Request 读写基本上差不多
	sync.Mutex
	seqNum2Request map[int32]*Request
	currentSeqId   int32 // 范围: 1 ~ 100000
	State          ConnState
	Index          int
	delegate       BackendConnStateChanged
}

func NewBackendConn(addr string, delegate BackendConnStateChanged) *BackendConn {
	bc := &BackendConn{
		addr:           addr,
		input:          make(chan *Request, 1024),
		readChan:       make(chan bool, 1024),
		seqNum2Request: make(map[int32]*Request, 4096),
		currentSeqId:   1,
		State:          ConnStateInit,
		Index:          -1,
		delegate:       delegate,
	}
	go bc.Run()
	return bc
}

//
// 不断建立到后端的逻辑，负责: BackendConn#input到redis的数据的输入和返回
//
func (bc *BackendConn) Run() {
	log.Infof("backend conn [%p] to %s, start service", bc, bc.addr)
	for k := 0; ; k++ {
		// 1. 首先BackendConn将当前 input中的数据写到后端服务中
		err := bc.loopWriter()
		// 从Active切换到非正常状态
		if bc.State == ConnStateActive && bc.delegate != nil {
			bc.State = ConnStateFailed
			bc.delegate.StateChanged(bc) // 通知其他人状态出现问题
		} else {
			bc.State = ConnStateFailed
		}

		if err == nil {
			break
		} else {

			// 如果出现err, 则将bc.input中现有的数据都flush回去（直接报错)
			for i := len(bc.input); i != 0; i-- {
				r := <-bc.input
				bc.setResponse(r, nil, err)
			}
		}

		// 然后等待，继续尝试新的连接
		log.WarnErrorf(err, "backend conn [%p] to %s, restart [%d]", bc, bc.addr, k)
		time.Sleep(time.Millisecond * 50)
	}
	log.Infof("backend conn [%p] to %s, stop and exit", bc, bc.addr)
}

func (bc *BackendConn) Addr() string {
	return bc.addr
}

func (bc *BackendConn) Close() {
	bc.stop.Do(func() {
		close(bc.input)
	})
}

func (bc *BackendConn) PushBack(r *Request) {
	if r.Wait != nil {
		r.Wait.Add(1)
	}
	bc.input <- r
}

func (bc *BackendConn) KeepAlive() bool {
	return false
	//	if len(bc.input) != 0 {
	//		return false
	//	}
	//	r := &Request{
	//		Resp: redis.NewArray([]*redis.Resp{
	//			redis.NewBulkBytes([]byte("PING")),
	//		}),
	//	}

	//	select {
	//	case bc.input <- r:
	//		return true
	//	default:
	//		return false
	//	}
}

var ErrFailedRequest = errors.New("discard failed request")

func (bc *BackendConn) loopWriter() error {
	// bc.input 实现了前段请求的buffer
	r, ok := <-bc.input

	if ok {
		c, err := bc.newBackendReader()

		// 如果出错，则表示连接Redis或授权出问题了
		if err != nil {
			return bc.setResponse(r, nil, err)
		}

		// 现在进入可用状态
		bc.State = ConnStateActive
		if bc.delegate != nil {
			bc.delegate.StateChanged(bc)
		}

		for ok {
			// 如果暂时没有数据输入，则p策略可能就有问题了
			// 只有写入数据，才有可能产生flush; 如果是最后一个数据必须自己flush, 否则就可能无限期等待
			//
			var flush = len(bc.input) == 0
			fmt.Printf("Force flush %t\n", flush)

			if bc.canForward(r) {

				// 1. 替换新的SeqId
				r.ReplaceSeqId(bc.currentSeqId)

				// 2. 主动控制Buffer的flush
				c.Write(r.Request.Data)
				err := c.FlushBuffer(flush)

				if err == nil {
					log.Printf("Succeed Write Request to backend Server/LB\n")
					bc.IncreaseCurrentSeqId()
					bc.Lock()
					if len(bc.seqNum2Request) != 0 {
						log.Printf(Red("Invalid SeqNum2Request Size: %d\n"), len(bc.seqNum2Request))
						for k, v := range bc.seqNum2Request {
							log.Printf(Red("K: %d, V: %p-%v\n"), k, v, v)
						}

					}
					bc.seqNum2Request[r.Response.SeqId] = r
					bc.Unlock()

					bc.readChan <- true

					// 读取
					r, ok = <-bc.input
					continue
				}
			}

			// 进入不可用状态(不可用状态下，通过自我心跳进入可用状态)
			bc.State = ConnStateFailed
			if bc.delegate != nil {
				bc.delegate.StateChanged(bc)
			}

			if err := c.FlushTransport(flush); err != nil {
				return bc.setResponse(r, nil, err)
			}

			// 请求压根就没有发送
			bc.setResponse(r, nil, ErrFailedRequest)

			// 继续读取请求, 如果有异常，如何处理呢?
			r, ok = <-bc.input
			break
		}
	}
	return nil
}

func (bc *BackendConn) IncreaseCurrentSeqId() {
	// 备案(只有loopWriter操作，不加锁)
	bc.currentSeqId++
	if bc.currentSeqId > 100000 {
		bc.currentSeqId = 1
	}
}

// 创建一个到"后端服务"的连接
func (bc *BackendConn) newBackendReader() (*TBufferedFramedTransport, error) {

	// 创建连接(只要IP没有问题， err一般就是空)
	socket, err := thrift.NewTSocketTimeout(bc.addr, time.Hour*3)

	log.Printf(Cyan("Create Socket To: %s\n"), bc.addr)

	if err != nil {
		log.ErrorErrorf(err, "Create Socket Failed: %v, Addr: %s\n", err, bc.addr)
		// 连接不上，失败
		return nil, err
	}

	// 只要服务存在，一般不会出现err
	err = socket.Open()
	if err != nil {
		log.ErrorErrorf(err, "Socket Open Failed: %v, Addr: %s\n", err, bc.addr)
		// 连接不上，失败
		return nil, err
	} else {
		log.Printf("Socket Open Succedd\n")
	}

	c := NewTBufferedFramedTransport(socket, 100*time.Microsecond, 20)
	//	c.Open()

	go func() {
		defer c.Close()

		for true {
			// 读取来自后端服务的数据
			// client <---> proxy <-----> backend_conn <---> rpc_server
			// ReadFrame需要有一个度? 如果碰到EOF该如何处理呢?

			// io.EOF在两种情况下会出现
			//
			<-bc.readChan
			resp, err := c.ReadFrame()

			if err != nil {
				log.ErrorErrorf(err, Red("ReadFrame From Server with Error: %v\n"), err)
				bc.flushRequests(err)
				break
			} else {
				bc.setResponse(nil, resp, err)
			}
		}
	}()
	return c, nil
}

// 处理所有的等待中的请求
func (bc *BackendConn) flushRequests(err error) {
	// 告诉BackendService, 不再接受新的请求
	if bc.delegate != nil {
		bc.delegate.StateChanged(bc)
	}

	bc.Lock()
	seqRequest := bc.seqNum2Request
	bc.seqNum2Request = make(map[int32]*Request, 4096)
	bc.Unlock()

	for _, request := range seqRequest {
		log.Printf(Red("Handle Failed Request: %s %s"), request.Service, request.Request.Name)
		request.Response.Err = err
		request.Wait.Done()
	}

}

func (bc *BackendConn) canForward(r *Request) bool {
	return bc.State == ConnStateActive
	//	if r.Failed != nil && r.Failed.Get() {
	//		return false
	//	} else {
	//		return true
	//	}
}

// 配对 Request, resp, err
// PARAM: resp []byte 为一帧完整的thrift数据包
func (bc *BackendConn) setResponse(r *Request, data []byte, err error) error {
	// 表示出现错误了
	if data == nil {
		log.Printf("No Data From Server, error: %v\n", err)
		r.Response.Err = err
	} else {
		// 从resp中读取基本的信息
		typeId, seqId, err := DecodeThriftTypIdSeqId(data)

		// 解码错误，直接报错
		if err != nil {
			return err
		}

		// 找到对应的Request
		bc.Lock()
		req, ok := bc.seqNum2Request[seqId]
		if ok {
			delete(bc.seqNum2Request, seqId)
		}
		bc.Unlock()

		if !ok {
			return errors.New("Invalid Response")
		}

		log.Printf("Data From Server, seqId: %d, Request: %d\n", seqId, req.Request.SeqId)
		r = req
		r.Response.TypeId = typeId
	}

	r.Response.Data, r.Response.Err = data, err

	// 还原SeqId
	if data != nil {
		r.RestoreSeqId()
	}

	// 设置几个控制用的channel
	if err != nil && r.Failed != nil {
		r.Failed.Set(true)
	}
	if r.Wait != nil {
		r.Wait.Done()
	}
	//	if r.slot != nil {
	//		r.slot.Done()
	//	}
	return err
}

////
//// 通过引用计数管理后端的BackendConn
////
//type SharedBackendConn struct {
//	*BackendConn
//	mu sync.Mutex

//	refcnt int
//}

//func NewSharedBackendConn(addr string, delegate BackendConnStateChanged) *SharedBackendConn {
//	return &SharedBackendConn{BackendConn: NewBackendConn(addr, delegate), refcnt: 1}
//}

//func (s *SharedBackendConn) Close() bool {
//	s.mu.Lock()
//	defer s.mu.Unlock()
//	if s.refcnt <= 0 {
//		log.Panicf("shared backend conn has been closed, close too many times")
//	}
//	if s.refcnt == 1 {
//		s.BackendConn.Close()
//	}
//	s.refcnt--
//	return s.refcnt == 0
//}

//// Close之后不能再引用
//// socket不能多次打开，必须重建
////
//func (s *SharedBackendConn) IncrRefcnt() {
//	s.mu.Lock()
//	defer s.mu.Unlock()
//	if s.refcnt == 0 {
//		log.Panicf("shared backend conn has been closed")
//	}
//	s.refcnt++
//}

func FormatYYYYmmDDHHMMSS(date time.Time) string {
	return date.Format("@2006-01-02 15:04:05")
}
