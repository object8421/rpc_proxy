package proxy

import (
	"fmt"
	rpc_commons "git.chunyu.me/infra/rpc_commons"
	utils "git.chunyu.me/infra/rpc_proxy/utils"
	"git.chunyu.me/infra/rpc_proxy/utils/log"
	zk "git.chunyu.me/infra/rpc_proxy/zk"
	zmq "github.com/pebbe/zmq4"
	"strings"
	"time"
)

const (
	HEARTBEAT_INTERVAL = 1000 * time.Millisecond
)

type ProxyServer struct {
	ProductName  string
	FrontendAddr string
	ZkAdresses   string
	Verbose      bool
	Profile      bool
}

func NewProxyServer(config *utils.Config) *ProxyServer {
	server := &ProxyServer{
		ProductName:  config.ProductName,
		FrontendAddr: config.FrontendAddr,
		ZkAdresses:   config.ZkAddr,
		Verbose:      config.Verbose,
		Profile:      config.Profile,
	}
	return server
}

//
// 两参数是必须的:  ProductName, zkAddress, frontAddr可以用来测试
//
func (p *ProxyServer) Run() {
	// 1. 创建到zk的连接
	var topo *zk.Topology
	topo = zk.NewTopology(p.ProductName, p.ZkAdresses)

	// 3. 读取后端服务的配置
	poller := zmq.NewPoller()
	backServices := NewBackServices(poller, p.ProductName, topo, p.Verbose)

	// 4. 创建前端服务
	frontend, _ := zmq.NewSocket(zmq.ROUTER)
	defer frontend.Close()

	// ROUTER/ROUTER绑定到指定的端口
	log.Println("---->Bind: ", rpc_commons.Magenta(p.FrontendAddr))
	frontend.Bind(p.FrontendAddr) //  For clients

	// 开始监听前端服务
	poller.Add(frontend, zmq.POLLIN)

	for {
		var sockets []zmq.Polled
		var err error

		sockets, err = poller.Poll(HEARTBEAT_INTERVAL)

		if err != nil {
			log.Println("Encounter Errors, Services Stoped: ", err)
			continue
		}

		for _, socket := range sockets {
			switch socket.Socket {

			case frontend:
				if p.Verbose {
					log.Println("----->Message from front: ")
				}
				msgs, err := frontend.RecvMessage(0)

				if err != nil {
					continue //  Interrupted
				}
				var service string
				var client_id string

				utils.PrintZeromqMsgs(msgs, "ProxyFrontEnd")

				// msg格式: <client_id, '', service,  '', other_msgs>
				client_id, msgs = utils.Unwrap(msgs)
				service, msgs = utils.Unwrap(msgs)

				//				log.Println("Client_id: ", client_id, ", Service: ", service)

				backService := backServices.GetBackService(service)

				if backService == nil {
					log.Println("BackService Not Found...")
					// 最后一个msg为Thrift编码后的消息
					thriftMsg := msgs[len(msgs)-1]
					// XXX: seqId如果不需要，也可以使用固定的数字
					_, _, seqId, _ := ParseThriftMsgBegin([]byte(thriftMsg))
					errMsg := GetServiceNotFoundData(service, seqId)

					// <client_id, "", errMsg>
					if len(msgs) > 1 {
						frontend.SendMessage(client_id, "", msgs[0:len(msgs)-1], errMsg)
					} else {
						frontend.SendMessage(client_id, "", errMsg)
					}

				} else {
					// <"", client_id, "", msgs>
					if p.Profile {
						lastMsg := msgs[len(msgs)-1]
						msgs = msgs[0 : len(msgs)-1]
						msgs = append(msgs, fmt.Sprintf("%.4f", float64(time.Now().UnixNano())*1e-9), "", lastMsg)
						if p.Verbose {
							log.Println(printList(msgs))
						}
					}
					total, err, errMsg := backService.HandleRequest(client_id, msgs)
					if errMsg != nil {
						if p.Verbose {
							log.Println("backService Error for service: ", service)
						}
						if len(msgs) > 1 {
							frontend.SendMessage(client_id, "", msgs[0:len(msgs)-1], *errMsg)
						} else {
							frontend.SendMessage(client_id, "", *errMsg)
						}
					} else if err != nil {
						log.Println(utils.Red("backService.HandleRequest Error: "), err, ", Total: ", total)
					}
				}
			default:
				// 除了来自前端的数据，其他的都来自后端
				msgs, err := socket.Socket.RecvMessage(0)
				if err != nil {
					log.Println("Encounter Errors When receiving from background")
					continue //  Interrupted
				}
				if p.Verbose {
					utils.PrintZeromqMsgs(msgs, "proxy")
				}

				msgs = utils.TrimLeftEmptyMsg(msgs)

				// msgs格式: <client_id, "", rpc_data>
				//          <control_msg_rpc_data>
				if len(msgs) == 1 {
					// 告知后端的服务可能有问题

				} else {
					if p.Profile {
						lastMsg := msgs[len(msgs)-1]
						msgs = msgs[0 : len(msgs)-1]
						msgs = append(msgs, fmt.Sprintf("%.4f", float64(time.Now().UnixNano())*1e-9), "", lastMsg)
					}
					if p.Verbose {
						log.Println(printList(msgs))
					}
					frontend.SendMessage(msgs)
				}
			}
		}
	}
}

func printList(msgs []string) string {
	results := make([]string, 0, len(msgs))
	results = append(results, fmt.Sprintf("Msgs Len: %d, ", len(msgs)-1))
	for i := 0; i < len(msgs)-1; i++ {
		results = append(results, fmt.Sprintf("[%s]", msgs[i]))
	}
	return strings.Join(results, ",")
}
