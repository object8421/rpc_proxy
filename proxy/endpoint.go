//// Copyright 2015 Spring Rain Software Compnay LTD. All Rights Reserved.
//// Licensed under the MIT (MIT-LICENSE.txt) license.
package proxy

import (
	"encoding/json"
	zookeeper "git.chunyu.me/infra/go-zookeeper/zk"
	"git.chunyu.me/infra/rpc_proxy/utils/log"
	zk "git.chunyu.me/infra/rpc_proxy/zk"
	"git.chunyu.me/infra/zkhelper"
	os_path "path"
)

type ServiceEndpoint struct {
	Service   string `json:"service"`
	ServiceId string `json:"service_id"`
	Frontend  string `json:"frontend"`
}

func NewServiceEndpoint(service string, serviceId string, frontend string) *ServiceEndpoint {
	return &ServiceEndpoint{
		Service:   service,
		ServiceId: serviceId,
		Frontend:  frontend,
	}
}

//
// 删除Service Endpoint
//
func (s *ServiceEndpoint) DeleteServiceEndpoint(top *zk.Topology) {
	path := top.ProductServiceEndPointPath(s.Service, s.ServiceId)
	zkhelper.DeleteRecursive(top.ZkConn, path, -1)

	log.Println(Red("DeleteServiceEndpoint"), "Path: ", path)
}

//
// 注册一个服务的Endpoints
//
func (s *ServiceEndpoint) AddServiceEndpoint(topo *zk.Topology) error {
	path := topo.ProductServiceEndPointPath(s.Service, s.ServiceId)
	data, err := json.Marshal(s)
	if err != nil {
		return err
	}

	// 创建Service(XXX: Service本身不包含数据)
	zk.CreateRecursive(topo.ZkConn, os_path.Dir(path), "", 0, zkhelper.DefaultDirACLs())

	// 当前的Session挂了，服务就下线
	// topo.FlagEphemeral

	// 参考： https://www.box.com/blog/a-gotcha-when-using-zookeeper-ephemeral-nodes/
	// 如果之前的Session信息还存在，则先删除；然后再添加
	topo.ZkConn.Delete(path, -1)
	var pathCreated string
	pathCreated, err = topo.ZkConn.Create(path, []byte(data), int32(zookeeper.FlagEphemeral), zkhelper.DefaultFileACLs())

	log.Println(Green("AddServiceEndpoint"), "Path: ", pathCreated, ", Error: ", err)
	return err
}

func GetServiceEndpoint(top *zk.Topology, service string, serviceId string) (endpoint *ServiceEndpoint, err error) {

	path := top.ProductServiceEndPointPath(service, serviceId)
	data, _, err := top.ZkConn.Get(path)
	if err != nil {
		return nil, err
	}
	endpoint = &ServiceEndpoint{}
	err = json.Unmarshal(data, endpoint)
	if err != nil {
		return nil, err
	} else {
		return endpoint, nil
	}
}
