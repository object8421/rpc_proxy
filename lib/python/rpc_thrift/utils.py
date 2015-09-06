# -*- coding: utf-8 -*-
from __future__ import absolute_import

from thrift.protocol.TMultiplexedProtocol import TMultiplexedProtocol

from rpc_thrift.protocol import TUtf8BinaryProtocol, TLoggerMultiplexedProtocol
from rpc_thrift.transport import TAutoConnectFramedTransport, TSocket




# Client目前只考虑单线程的情况, 如果是多线程，或者coroutine可能需要使用pool
_base_protocol = None
def get_base_protocol(endpoint, timeout=5000):
    global _base_protocol
    if not _base_protocol:
        if endpoint.find(":") != -1:
            hostport = endpoint.split(":")
            host = hostport[0]
            port = int(hostport[1])
            unix_socket = None
        else:
            host = None
            port = None
            unix_socket = endpoint

        socket = TSocket(host=host, port=port, unix_socket=unix_socket)
        socket.setTimeout(timeout)
        transport = TAutoConnectFramedTransport(socket)
        _base_protocol = TUtf8BinaryProtocol(transport)
    return _base_protocol


def get_base_protocol_4_pool(endpoint, timeout=5000):
    if endpoint.find(":") != -1:
        hostport = endpoint.split(":")
        host = hostport[0]
        port = int(hostport[1])
        unix_socket = None
    else:
        host = None
        port = None
        unix_socket = endpoint
    socket = TSocket(host=host, port=port, unix_socket=unix_socket)
    socket.setTimeout(timeout)
    transport = TAutoConnectFramedTransport(socket)

    return TUtf8BinaryProtocol(transport)


def get_service_protocol(service, base_protocol=None, logger=None):
    """
    多个不同的service可以共用一个base_protocol; 如果指定了service, 则在base_protocol的基础上添加一个新的wrap

    TSocket --> TAutoConnectSocket --> TFramedTransport --> TUtf8BinaryProtocol --> TMultiplexedProtocol
                    |
                    |-----------------------------------> 直接合并成为: TAutoConnectFramedTransport, 之后的protocol最好不要带有状态

    存在问题: TFramedTransport存在buf, 自动重连之后状态还存在

    :param service:
    :param base_protocol:
    :return:
    """
    base_protocol = base_protocol or _base_protocol
    if service:
        if logger:
            return TLoggerMultiplexedProtocol(base_protocol, service, logger)
        else:
            return TMultiplexedProtocol(base_protocol, service)
    else:
        return base_protocol