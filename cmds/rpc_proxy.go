package main

import (
	rpc_commons "git.chunyu.me/infra/rpc_commons"
	proxy "git.chunyu.me/infra/rpc_proxy/proxy"
	utils "git.chunyu.me/infra/rpc_proxy/utils"
	"git.chunyu.me/infra/rpc_proxy/utils/bytesize"
	"git.chunyu.me/infra/rpc_proxy/utils/log"
	"github.com/docopt/docopt-go"
	"os"
)

const (
	PROXY_FRONT_END = "rpc_front"
)

var usage = `usage: rpc_proxy -c <config_file> [-L <log_file>] [--log-level=<loglevel>] [--log-filesize=<filesize>] 

options:
   -c <config_file>
   -L	set output log file, default is stdout
   --log-level=<loglevel>	set log level: info, warn, error, debug [default: info]
   --log-filesize=<maxsize>  set max log file size, suffixes "KB", "MB", "GB" are allowed, 1KB=1024 bytes, etc. Default is 1GB.
`

//
// Proxy关闭，则整个机器就OVER, 需要考虑将整个机器下线
// 因此Proxy需要设计的非常完美，不要轻易地被杀死，或自杀
//
func main() {
	// 解析输入参数
	args, err := docopt.Parse(usage, nil, true, "Chunyu RPC Local Proxy v0.1", true)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	var maxFileFrag = 2
	var maxFragSize int64 = bytesize.GB * 1
	if s, ok := args["--log-filesize"].(string); ok && s != "" {
		v, err := bytesize.Parse(s)
		if err != nil {
			log.PanicErrorf(err, "invalid max log file size = %s", s)
		}
		maxFragSize = v
	}

	// set output log file
	if s, ok := args["-L"].(string); ok && s != "" {
		f, err := log.NewRollingFile(s, maxFileFrag, maxFragSize)
		if err != nil {
			log.PanicErrorf(err, "open rolling log file failed: %s", s)
		} else {
			defer f.Close()
			log.StdLog = log.New(f, "")
		}
	}
	log.SetLevel(log.LEVEL_INFO)
	log.SetFlags(log.Flags() | log.Lshortfile)

	// set log level
	if s, ok := args["--log-level"].(string); ok && s != "" {
		rpc_commons.SetLogLevel(s)
	}

	// 从config文件中读取数据

	configFile := args["-c"].(string)
	conf, err := utils.LoadConf(configFile)
	if err != nil {
		log.PanicErrorf(err, "load config failed")
	}

	if conf.ProductName == "" {
		log.PanicErrorf(err, "Invalid ProductName")
	}
	if conf.ZkAddr == "" {
		log.PanicErrorf(err, "Invalid zookeeper address")
	}
	if conf.FrontendAddr == "" {
		log.PanicErrorf(err, "Invalid Proxy address")
	}

	// 正式的服务
	server := proxy.NewProxyServer(conf)
	server.Run()
}
