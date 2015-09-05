include "rpc_thrift.services.thrift"
namespace java service.chunyu.demo.echo
service EchoService extends rpc_thrift.services.RpcServiceBase {
    string echo(1:string msg),
}