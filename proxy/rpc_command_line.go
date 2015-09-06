//// Copyright 2015 Spring Rain Software Compnay LTD. All Rights Reserved.
//// Licensed under the MIT (MIT-LICENSE.txt) license.
package proxy

import (
	"fmt"
	utils "git.chunyu.me/infra/rpc_proxy/utils"
	"git.chunyu.me/infra/rpc_proxy/utils/log"
	"github.com/docopt/docopt-go"
	"os"
	"strconv"
)

var usage = `Usage: 
  %s -c <config_file> [-L <log_file>] [--log-level=<loglevel>] [--log-keep-days=<maxdays>]
  %s -V | --version

options:
   -c <config_file>
   -L	set output log file, default is stdout
   --log-level=<loglevel>	set log level: info, warn, error, debug [default: info]
   --log-keep-days=<maxdays>  set max log file keep days, default is 3 days
`

func RpcMain(binaryName string, serviceDesc string, configCheck ConfigCheck,
	serverFactory ServerFactorory, buildDate string, gitVersion string) {

	// 1. 准备解析参数
	usage = fmt.Sprintf(usage, binaryName, binaryName)

	version := fmt.Sprintf("Version: %s\nBuildDate: %s\nDesc: %s\nAuthor:wangfei@chunyu.me", gitVersion, buildDate, serviceDesc)
	args, err := docopt.Parse(usage, nil, true, version, true)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if s, ok := args["-V"].(bool); ok && s {
		fmt.Println(version)
		os.Exit(1)
	}

	// 2. 解析Log相关的配置
	log.SetLevel(log.LEVEL_INFO)

	var maxKeepDays int = 3
	if s, ok := args["--log-keep-days"].(string); ok && s != "" {
		v, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			log.PanicErrorf(err, "invalid max log file keep days = %s", s)
		}
		maxKeepDays = int(v)
	}

	// set output log file
	if s, ok := args["-L"].(string); ok && s != "" {
		f, err := log.NewRollingFile(s, maxKeepDays)
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
