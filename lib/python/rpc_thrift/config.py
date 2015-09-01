# -*- coding: utf-8 -*-
from __future__ import absolute_import

# 配置文件相关的参数
import sys
import traceback

RPC_DEFAULT_CONFIG = "config.ini"

RPC_ZK = "zk"
RPC_ZK_TIMEOUT = "zk_session_timeout"

RPC_PRODUCT = "product"
RPC_SERVICE = "service"

RPC_FRONT_HOST = "front_host"
RPC_FRONT_PORT = "front_port"
RPC_IP_PREFIX = "ip_prefix"

RPC_BACK_ADDRESS = "back_address"

RPC_WORKER_POOL_SIZE = "worker_pool_size"
RPC_PROXY_ADDRESS  = "proxy_address"


def parse_config(config_path):
    config = {}
    for line in open(config_path, "r").readlines():
        line = line.strip()
        if not line or line.find("#") != -1:
            continue
        items = line.split("=")
        if len(items) >= 2:
            config[items[0]] = items[1]
    return config

def print_exception():
    '''
    直接输出异常信息
    '''
    exc_type, exc_value, exc_traceback = sys.exc_info()
    exc = traceback.format_exception(exc_type, exc_value, exc_traceback)

    # 以人可以读的方式打印Log
    print "-------------------------"
    print "".join(exc)