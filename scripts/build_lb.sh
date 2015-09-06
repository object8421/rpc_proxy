go build -ldflags "-X main.buildDate=`date -u +%Y%m%d%H%M%S` -X main.gitVersion=`git -C git.chunyu.me/infra/rpc_proxy rev-parse HEAD`" git.chunyu.me/infra/rpc_proxy/cmds/rpc_lb.go 
#&& cp rpc_lb /usr/local/rpc_proxy/bin/
