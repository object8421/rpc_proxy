package proxy

import (
	"fmt"
	"io"
	"sync"
	"time"

	thrift "git.apache.org/thrift.git/lib/go/thrift"
	"git.chunyu.me/infra/rpc_proxy/utils/errors"
	"git.chunyu.me/infra/rpc_proxy/utils/log"
)

type BackendConnStateChanged interface {
	StateChanged(conn *BackendConn)
}

type BackendConn struct {
	addr string
	stop sync.Once

	input chan *Request // 输入的请求, 有: 1024个Buffer

	// seqNum2Request 读写基本上差不多
	sync.Mutex
	seqNum2Request map[int32]*Request
	currentSeqId   int32 // 范围: 1 ~ 100000

	Index         int
	delegate      BackendConnStateChanged
	ticker        *time.Ticker
	lastHbTime    int64
	IsMarkOffline bool // 是否标记下线
	IsConnActive  bool // 是否处于Active状态呢
	verbose       bool
}

func NewBackendConn(addr string, delegate BackendConnStateChanged, verbose bool) *BackendConn {
	bc := &BackendConn{
		addr:           addr,
		input:          make(chan *Request, 1024),
		seqNum2Request: make(map[int32]*Request, 4096),
		currentSeqId:   1,
		Index:          -1,
		delegate:       delegate,
		IsConnActive:   false,
		IsMarkOffline:  false,
		verbose:        verbose,
	}
	go bc.Run()
	go bc.Heartbeat()
	return bc
}

func (bc *BackendConn) Heartbeat() {
	bc.ticker = time.NewTicker(time.Second)
	bc.lastHbTime = time.Now().Unix()
	for true {
		select {
		case <-bc.ticker.C:
			if time.Now().Unix()-bc.lastHbTime > 4 {
				bc.MarkConnActiveFalse()
			}
			// 定时添加Ping的任务
			r := NewPingRequest(0)
			bc.PushBack(r)
		}
	}
}

func (bc *BackendConn) MarkOffline() {
	log.Printf(Red("BackendConn: %s MarkOffline\n"), bc.addr)

	bc.IsMarkOffline = true
}

func (bc *BackendConn) MarkConnActiveFalse() {
	// 从Active切换到非正常状态
	if bc.IsConnActive && bc.delegate != nil {
		bc.IsConnActive = false
		bc.delegate.StateChanged(bc) // 通知其他人状态出现问题
	} else {
		bc.IsConnActive = false
	}
}

//
// 从Active切换到非正常状态
//
func (bc *BackendConn) MarkConnActiveOK() {
	if !bc.IsMarkOffline {
		bc.IsConnActive = true
		if bc.delegate != nil {
			bc.delegate.StateChanged(bc) // 通知其他人状态出现问题
		}
	}
}

//
// 不断建立到后端的逻辑，负责: BackendConn#input到redis的数据的输入和返回
//
func (bc *BackendConn) Run() {
	log.Infof("backend conn [%p] to %s, start service", bc, bc.addr)

	for k := 0; !bc.IsMarkOffline; k++ {

		// 1. 首先BackendConn将当前 input中的数据写到后端服务中
		err := bc.loopWriter()
		// 标记状态异常
		bc.MarkConnActiveFalse()

		if err == nil {
			log.Println(Red("BackendConn#loopWriter normal Exit..."))
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
		bc.MarkConnActiveOK()

		for ok {
			// 如果暂时没有数据输入，则p策略可能就有问题了
			// 只有写入数据，才有可能产生flush; 如果是最后一个数据必须自己flush, 否则就可能无限期等待
			//
			if r.Request.TypeId == MESSAGE_TYPE_HEART_BEAT {
				// 过期的HB信号，直接放弃
				if time.Now().Unix()-r.Start > 4 {
					r, ok = <-bc.input
					continue
				} else {
					log.Printf(Magenta("Send Heartbeat to %s\n"), bc.Addr())
				}
			}
			// 如果有5s没有收到HB信号，则报错
			if time.Now().Unix()-bc.lastHbTime > 5 {
				return bc.setResponse(r, nil, errors.New("HB timeout"))
			}

			var flush = len(bc.input) == 0
			fmt.Printf("Force flush %t\n", flush)

			// 1. 替换新的SeqId
			r.ReplaceSeqId(bc.currentSeqId)

			// 2. 主动控制Buffer的flush
			c.Write(r.Request.Data)
			err := c.FlushBuffer(flush)

			if err == nil {
				log.Printf("Succeed Write Request to backend Server/LB\n")
				bc.IncreaseCurrentSeqId()
				bc.Lock()
				bc.seqNum2Request[r.Response.SeqId] = r
				bc.Unlock()

				// 读取
				r, ok = <-bc.input
				continue
			}

			// 进入不可用状态(不可用状态下，通过自我心跳进入可用状态)
			bc.MarkConnActiveFalse()

			return bc.setResponse(r, nil, err)
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

	go func() {
		defer c.Close()

		for true {
			// 读取来自后端服务的数据，通过 setResponse 转交给 前端
			// client <---> proxy <-----> backend_conn <---> rpc_server
			// ReadFrame需要有一个度? 如果碰到EOF该如何处理呢?

			// io.EOF在两种情况下会出现
			//
			resp, err := c.ReadFrame()

			if err != nil {
				if err != io.EOF && err.Error() != "EOF" {
					log.ErrorErrorf(err, Red("ReadFrame From Server with Error: %v\n"), err)
				}
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
	bc.MarkConnActiveFalse()

	bc.Lock()
	seqRequest := bc.seqNum2Request
	bc.seqNum2Request = make(map[int32]*Request, 4096)
	bc.Unlock()

	for _, request := range seqRequest {
		log.Printf(Red("Handle Failed Request: %s %s"), request.Service, request.Request.Name)
		request.Response.Err = err
		if request.Wait != nil {
			request.Wait.Done()
		}
	}

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

		// 如果是心跳，则OK
		if typeId == MESSAGE_TYPE_HEART_BEAT {
			log.Printf(Magenta("Get Ping/Pang Back\n"))
			bc.lastHbTime = time.Now().Unix()
			return nil
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

	return err
}

func FormatYYYYmmDDHHMMSS(date time.Time) string {
	return date.Format("@2006-01-02 15:04:05")
}
