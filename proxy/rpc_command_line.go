//// Copyright 2015 Spring Rain Software Compnay LTD. All Rights Reserved.
//// Licensed under the MIT (MIT-LICENSE.txt) license.
package proxy

import (
	"fmt"
	utils "git.chunyu.me/infra/rpc_proxy/utils"
	"git.chunyu.me/infra/rpc_proxy/utils/bytesize"
	"git.chunyu.me/infra/rpc_proxy/utils/log"
	"github.com/docopt/docopt-go"
	"os"
)

var usage = `usage: %s -c <config_file>[-L <log_file>] [--log-level=<loglevel>] [--log-filesize=<filesize>] 

options:
   -c <config_file>
   -L	set output log file, default is stdout
   --log-level=<loglevel>	set log level: info, warn, error, debug [default: info]
   --log-filesize=<maxsize>  set max log file size, suffixes "KB", "MB", "GB" are allowed, 1KB=1024 bytes, etc. Default is 1GB.
`

func RpcMain(binaryName string, serviceDesc string, configCheck ConfigCheck,
	serverFactory ServerFactorory) {

	// 1. 准备解析参数
	usage = fmt.Sprintf(usage, binaryName)
	args, err := docopt.Parse(usage, nil, true, serviceDesc, true)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	// 2. 解析Log相关的配置
	log.SetLevel(log.LEVEL_INFO)

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
		SetLogLevel(s)
	}

	// 3. 解析Config
	configFile := args["-c"].(string)
	conf, err := utils.LoadConf(configFile)
	if err != nil {
		log.PanicErrorf(err, "load config failed")
	}

	if configCheck != nil {
		configCheck(conf)
	} else {
		log.Panic("No Config Check Given")
	}

	// 启动服务
	server := serverFactory(conf)
	server.Run()
}
