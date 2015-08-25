//
//  Paranoid Pirate queue. 参考: http://zguide.zeromq.org/php:chapter4
//
package main

import (
	"fmt"
	rpc_commons "git.chunyu.me/infra/rpc_commons"
	config "git.chunyu.me/infra/rpc_proxy/config"
	lb "git.chunyu.me/infra/rpc_proxy/lb"
	proxy "git.chunyu.me/infra/rpc_proxy/proxy"
	utils "git.chunyu.me/infra/rpc_proxy/utils"
	"git.chunyu.me/infra/rpc_proxy/utils/bytesize"
	"git.chunyu.me/infra/rpc_proxy/utils/log"
	zk "git.chunyu.me/infra/rpc_proxy/zk"
	"github.com/docopt/docopt-go"
	color "github.com/fatih/color"
	zmq "github.com/pebbe/zmq4"
	topozk "github.com/wandoulabs/go-zookeeper/zk"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

var usage = `usage: rpc_lb -c <config_file> [-L <log_file>] [--log-level=<loglevel>] [--log-filesize=<filesize>] 

options:
   -c <config_file>
   -L	set output log file, default is stdout
   --log-level=<loglevel>	set log level: info, warn, error, debug [default: info]
   --log-filesize=<maxsize>  set max log file size, suffixes "KB", "MB", "GB" are allowed, 1KB=1024 bytes, etc. Default is 1GB.
`

func main() {
	args, err := docopt.Parse(usage, nil, true, "Chunyu RPC Load Balance v0.1", true)
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
		setLogLevel(s)
	}

	// set config file
	configFile := args["-c"].(string)
	conf, err := utils.LoadConf(configFile)
	if err != nil {
		log.PanicErrorf(err, "load config failed")
	}

	backendAddr = conf.BackAddr
	serviceName = conf.Service

	zkAddr = conf.ZkAddr
	config.VERBOSE = conf.Verbose

	if conf.ProductName == "" {
		// 既没有config指定，也没有命令行指定，则报错
		log.PanicErrorf(err, "Invalid ProductName")
	}

	if conf.ZkAddr == "" {
		log.PanicErrorf(err, "Invalid zookeeper address")
	}

	if conf.ServiceName == "" {
		log.PanicErrorf(err, "Invalid ServiceName")
	}

	if conf.BackendAddr == "" {
		log.PanicErrorf(err, "Invalid backend address")
	}
	if conf.FrontendAddr == "" {
		log.PanicErrorf(err, "Invalid frontend address")
	}

	server := lb.NewLoadBalanceServer(conf)
	server.Run()
}
