namespace java service.chunyu.demo.rpc_proxy
exception RpcException {
  1: i32  code,
  2: string msg
}

service RpcServiceBase {
    void ping();
}