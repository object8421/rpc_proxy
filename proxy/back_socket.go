package proxy

import (
	"fmt"
	rpc_commons "git.chunyu.me/infra/rpc_commons"
	utils "git.chunyu.me/infra/rpc_proxy/utils"
	"git.chunyu.me/infra/rpc_proxy/utils/log"
	zmq "github.com/pebbe/zmq4"
	"os"
	"sync"
	"time"
)

type BackSocket struct {
	Socket            *zmq.Socket
	Addr              string
	index             int
	markedOfflineTime int64
	poller            *zmq.Poller
}

//
// 建立到后端的Socket
// 后端服务包含:
// 直接提供Rpc服务的go/java服务; 或者Load Balance等
//
func NewBackSocket(Addr string, index int, poller *zmq.Poller) *BackSocket {
	return &BackSocket{
		Socket:            nil,
		Addr:              Addr,
		index:             index,
		markedOfflineTime: 0,
		poller:            poller,
	}
}
func (p *BackSocket) SendMessage(parts ...interface{}) (total int, err error) {
	if p.Socket == nil {
		err := p.connect()
		if err != nil {
			log.Println("Socket Connect Failed")
			return 0, err
		}
	}

	return p.Socket.SendMessage(parts...)
}

// 在第一次使用时再连接
func (p *BackSocket) connect() error {
	var err error
	p.Socket, err = zmq.NewSocket(zmq.DEALER)
	if err == nil {

		socketSeq += 1

		// 如果在内网，则使用 10.xx的IP, 否则使用""
		ip := GetIpWithPrefix("10.")
		p.Socket.SetIdentity(fmt.Sprintf("proxy-%s-%d", ip, os.Getpid()))

		p.Socket.Connect(p.Addr)
		// 都只看数据的输入
		// 数据的输出经过异步处理，不用考虑时间的问题
		p.poller.Add(p.Socket, zmq.POLLIN)
		log.Printf("Socket To %s Create Succeed\n", p.Addr)
		return nil
	} else {
		log.Printf("Socket To %s Create Failed: %v\n", p.Addr, err)
		return err
	}
}

// Sockets中维持一堆的BackSocket
// 其中: [0, Active)这一段的为有效的Sockets, 为: active area
//      [Active, len(Sockets))之间为等待关闭的Socket, zk, 或者上游环节直接通知关闭，这样便从active area转移出来
type BackSockets struct {
	sync.RWMutex
	Sockets []*BackSocket
	Active  int
	Current int
	poller  *zmq.Poller
}

func NewBackSockets(poller *zmq.Poller) *BackSockets {
	item := &BackSockets{
		Sockets: make([]*BackSocket, 0),
		Active:  0,
		Current: 0,
		poller:  poller,
	}
	return item
}

// 交换两个Socket的位置
func (p *BackSockets) swap(s0 *BackSocket, s1 *BackSocket) {
	s0.index, s1.index = s1.index, s0.index
	p.Sockets[s0.index] = s0
	p.Sockets[s1.index] = s1
}

//
// 添加一个endpoint到BackSockets, 如果之前已经添加，则返回 false, 否则返回 true
//
func (p *BackSockets) addEndpoint(addr string) bool {
	for i := 0; i < p.Active; i++ {
		if p.Sockets[i].Addr == addr {
			return false
		}
	}
	total := len(p.Sockets)
	socket := NewBackSocket(addr, total, p.poller)
	p.Sockets = append(p.Sockets, socket)

	// 将新添加的Socket作为Active添加进去
	p.swap(p.Sockets[p.Active], socket)
	p.Active++
	return true
}

func FormatYYYYmmDDHHMMSS(date Time) {
	return date.Format("@2006-01-02 15:04:05")
}

//
// 删除过期的Endpoints
//
func (p *BackSockets) PurgeEndpoints() {
	// 没有需要删除的对象
	if p.Active == len(p.Sockets) {
		return
	}

	log.Printf(utils.Green("PurgeEndpoints, total[%d] --> active[%d]"), len(p.Sockets), p.Active)

	p.Lock()
	defer p.Unlock()

	now := time.Now().Unix()
	nowStr := FormatYYYYmmDDHHMMSS(time.Now())

	for i := p.Active; i < len(p.Sockets); i++ {
		// 逐步删除过期的Sockets
		current := p.Sockets[i]
		lastIndex := len(p.Sockets) - 1
		if now-current.markedOfflineTime > 5 {

			// 将i和最后一个元素交换
			p.swap(current, p.Sockets[lastIndex])

			// 关闭
			// current
			// 关闭旧的Socket
			log.Println(utils.Red("PurgeEndpoints#Purge Old Socket: "), current.Addr, nowStr)
			// 由Socket自己维护自己的状态
			// current.Socket.Close()

			p.Sockets[lastIndex] = nil
			p.Sockets = p.Sockets[0:lastIndex]

			i-- // 保持原位
		}

	}
}

//
// 将不在: addrSet中的endPoint标记为下线
//
func (p *BackSockets) UpdateEndpointAddrs(addrSet map[string]bool) {
	p.Lock()
	defer p.Unlock()

	var addr string
	for addr, _ = range addrSet {
		p.addEndpoint(addr)
	}

	now := FormatYYYYmmDDHHMMSS(time.Now())
	for i := 0; i < p.Active; i++ {
		if _, ok := addrSet[p.Sockets[i].Addr]; !ok {
			log.Println(utils.Red("MarkEndpointsOffline#Mark Backend Offline: "), p.Sockets[i].Addr, now)

			p.markOffline(p.Sockets[i])
			i--
		}
	}
}

//
// 标记下线(Not Thread Safe)
//
func (p *BackSockets) markOffline(s *BackSocket) {
	s.markedOfflineTime = time.Now().Unix()

	if s.index < p.Active {
		p.swap(s, p.Sockets[p.Active-1])
		p.Active -= 1
		log.Warnf("MarkoffLine Socket, Current Active: %d", p.Active)
	} else {
		panic("Invalid index")
	}
}

//
// 返回下一个可用的Socket
//
func (p *BackSockets) NextSocket() *BackSocket {
	p.RLock()
	defer p.RUnlock()
	if p.Active > 0 {
		if p.Current >= p.Active {
			p.Current = 0
		}

		result := p.Sockets[p.Current]
		p.Current++
		return result
	}
	return nil
}
