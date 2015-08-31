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

type BackendConnLBStateChanged interface {
	StateChanged(conn *BackendConnLB)
}

type BackendConnLB struct {
	transport   thrift.TTransport
	addr4Log    string
	serviceName string
	input       chan *Request // 输入的请求, 有: 1024个Buffer

	// seqNum2Request 读写基本上差不多
	sync.Mutex
	seqNum2Request map[int32]*Request
	currentSeqId   int32 // 范围: 1 ~ 100000
	Index          int
	delegate       BackendConnLBStateChanged
	verbose        bool
	IsConnActive   bool // 是否处于Active状态呢

	hbLastTime int64
	hbTicker   *time.Ticker
	hbTimeout  chan bool
}

//
// LB(Load Balancer)以Server的形式和后端的服务(Backend)进行通信；
// 1. LB负责定期地和Backend进行ping/pang;
//    如果LB发现Backend长时间没有反应，或者出错，则端口和Backend之间的连接
// 2. Backend根据config.ini主动注册LB, 按照一定的策略重连
//
// BackendConnLB
//   1. 为Backend主动向LB注册之后，和LB之间建立的一条Connection
//   2. 底层的conn在LB BackendService中accepts时就已经建立好，因此BackendConnLB
//      就是建立在transport之上的控制逻辑
//
func NewBackendConnLB(transport thrift.TTransport, serviceName string, addr4Log string,
	delegate BackendConnLBStateChanged, verbose bool) *BackendConnLB {

	bc := &BackendConnLB{
		transport:      transport,
		addr4Log:       addr4Log,
		serviceName:    serviceName,
		input:          make(chan *Request, 1024),
		seqNum2Request: make(map[int32]*Request, 4096),
		currentSeqId:   1,
		Index:          -1,
		IsConnActive:   true, // 因为transport是刚刚建立的，因此直接认为该transport有效(以后可能需要添加有效性检测)
		delegate:       delegate,
		verbose:        verbose,
	}
	go bc.Run()
	return bc
}

//
// 发现心跳出现问题，就断开连接，结束ConnLB的生命周期
//
func (bc *BackendConnLB) Heartbeat() {
	go func() {
		bc.hbTicker = time.NewTicker(time.Second)
		bc.hbLastTime = time.Now().Unix()
		for true {
			select {
			case <-bc.hbTicker.C:
				if time.Now().Unix()-bc.hbLastTime > 6 {
					bc.hbTimeout <- true
				} else {
					if bc.IsConnActive {
						// 定时添加Ping的任务
						r := NewPingRequest(0)
						bc.PushBack(r)
					}
				}
			}
		}
	}()
}

func (bc *BackendConnLB) MarkConnActiveFalse() {
	// 从Active切换到非正常状态
	if bc.IsConnActive && bc.delegate != nil {
		bc.IsConnActive = false
		bc.delegate.StateChanged(bc) // 通知其他人状态出现问题
	} else {
		bc.IsConnActive = false
	}
}

// run之间 transport刚刚建立，因此服务的可靠性比较高
func (bc *BackendConnLB) Run() {
	log.Printf(Green("[%s]Add New BackendConnLB: %s\n"), bc.serviceName, bc.addr4Log)

	// 1. 首先BackendConn将当前 input中的数据写到后端服务中
	err := bc.loopWriter()

	// 2. 从Active切换到非正常状态, 同时不再从backend_service_lb接受新的任务
	//    可能出现异常，也可能正常退出(反正不干活了)
	bc.MarkConnActiveFalse()

	log.Printf(Red("[%s]Remove Faild BackendConnLB: %s\n"), bc.serviceName, bc.addr4Log)

	if err == nil {
		// bc.input被关闭了，应该就没有 Request 了
	} else {
		// 如果出现err, 则将bc.input中现有的数据都flush回去（直接报错)
		for i := len(bc.input); i != 0; i-- {
			r := <-bc.input
			bc.setResponse(r, nil, err)
		}
	}

}

func (bc *BackendConnLB) Addr4Log() string {
	return bc.addr4Log
}

func (bc *BackendConnLB) Close() {

}

//
// 将Request分配给BackendConnLB
//
func (bc *BackendConnLB) PushBack(r *Request) {
	// 关键路径必须有Log, 高频路径的Log需要受verbose状态的控制
	//	if bc.verbose {
	//		log.Printf("Add New Request To BackendConnLB: %s %s\n", r.Service, r.Request.Name)
	//	}

	r.Service = bc.serviceName

	if r.Wait != nil {
		r.Wait.Add(1)
	}
	bc.input <- r
}

//
// 数据: LB ---> backend services
//
// 如果input关闭，且loopWriter正常处理完毕之后，返回nil
// 其他情况返回error
//
func (bc *BackendConnLB) loopWriter() error {
	// 正常情况下, ok总是为True; 除非bc.input的发送者主动关闭了channel, 表示再也没有新的Task过来了
	// 参考: https://tour.golang.org/concurrency/4
	// 如果input没有关闭，则会block
	c := NewTBufferedFramedTransport(bc.transport, 100*time.Microsecond, 20)

	// bc.MarkConnActiveOK() // 准备接受数据
	// BackendConnLB 在构造之初就有打开的transport, 并且Active默认为OK

	bc.loopReader(c) // 异步
	bc.Heartbeat()   // 建立连接之后，就启动HB

	defer bc.hbTicker.Stop()

	var r *Request
	var ok bool

	for true {
		// 等待输入的Event, 或者 heartbeatTimeout
		select {
		case r, ok = <-bc.input:
			if !ok {
				return nil
			}
		case <-bc.hbTimeout:
			return errors.New("HB timeout")
		}

		// 如果暂时没有数据输入，则p策略可能就有问题了
		// 只有写入数据，才有可能产生flush; 如果是最后一个数据必须自己flush, 否则就可能无限期等待
		//
		if r.Request.TypeId == MESSAGE_TYPE_HEART_BEAT {
			// 过期的HB信号，直接放弃
			if time.Now().Unix()-r.Start > 4 {
				r, ok = <-bc.input
				continue
			} else {
				//					log.Printf(Magenta("Send Heartbeat to %s\n"), bc.Addr4Log())
			}
		}
		var flush = len(bc.input) == 0
		fmt.Printf("Force flush %t\n", flush)

		// 1. 替换新的SeqId
		r.ReplaceSeqId(bc.currentSeqId)

		// 2. 主动控制Buffer的flush

		//			log.Printf("Request Data Len: %d\n ", len(r.Request.Data))
		c.Write(r.Request.Data)
		err := c.FlushBuffer(flush)

		if err == nil {

			bc.IncreaseCurrentSeqId()
			bc.Lock()
			bc.seqNum2Request[r.Response.SeqId] = r
			bc.Unlock()

			// 继续读取请求, 如果有异常，如何处理呢?
		} else {
			log.ErrorErrorf(err, "FlushBuffer Error: %v\n", err)

			// 进入不可用状态(不可用状态下，通过自我心跳进入可用状态)
			return bc.setResponse(r, nil, err)
		}
	}
	return nil
}

//
// 从RPC Backend中读取结果, ReadFrame读取的是一个thrift message
// 存在两种情况:
// 1. 正常读取thrift message, 然后从frame解码得到seqId, 然后得到request, 结束请求
// 2. 读取错误
//    将现有的requests全部flush回去
//
func (bc *BackendConnLB) loopReader(c *TBufferedFramedTransport) {
	go func() {
		defer c.Close()

		for true {
			// 坚信: EOF只有在连接被关闭的情况下才会发生，其他情况下, Read等操作被会被block住
			// EOF有两种情况:
			// 1. 连接正常关闭，最后数据等完整读取 --> io.EOF
			// 2. 连接异常关闭，数据不完整 --> io.ErrUnexpectedEOF
			//
			// rpc_server ---> backend_conn
			frame, err := c.ReadFrame()

			if err != nil {
				if err != io.EOF && err.Error() != "EOF" {
					log.ErrorErrorf(err, Red("ReadFrame From rpc_server with Error: %v\n"), err)
				}
				bc.flushRequests(err)
				break
			} else {
				bc.setResponse(nil, frame, err)
			}
		}
	}()
}

// 处理所有的等待中的请求
func (bc *BackendConnLB) flushRequests(err error) {
	// 告诉BackendService, 不再接受新的请求
	bc.MarkConnActiveFalse()

	bc.Lock()
	seqRequest := bc.seqNum2Request
	bc.seqNum2Request = make(map[int32]*Request)
	bc.Unlock()

	for _, request := range seqRequest {
		if request.Request.TypeId == MESSAGE_TYPE_HEART_BEAT {
			// 心跳出错了，则直接直接跳过
		} else {
			log.Printf(Red("Handle Failed Request: %s.%s"), request.Service, request.Request.Name)
			request.Response.Err = err
			if request.Wait != nil {
				request.Wait.Done()
			}
		}
	}

	// 关闭输入
	close(bc.input)

}

// 配对 Request, resp, err
// PARAM: resp []byte 为一帧完整的thrift数据包
func (bc *BackendConnLB) setResponse(r *Request, data []byte, err error) error {
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
			bc.hbLastTime = time.Now().Unix()
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

func (bc *BackendConnLB) IncreaseCurrentSeqId() {
	// 备案(只有loopWriter操作，不加锁)
	bc.currentSeqId++
	if bc.currentSeqId > 100000 {
		bc.currentSeqId = 1
	}
}
