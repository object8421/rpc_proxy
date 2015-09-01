# -*- coding: utf-8 -*-
from __future__ import absolute_import
from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.protocol.TMultiplexedProtocol import TMultiplexedProtocol
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TFramedTransport
from rpc_thrift.transport import TAutoConnectSocket


# Client目前只考虑单线程的情况, 如果是多线程，或者coroutine可能需要使用pool
_base_protocol = None
def get_base_protocol(endpoint, timeout=2000):
    global _base_protocol
    if not _base_protocol:
        hostport = endpoint.split(":")
        socket = TSocket(hostport[0], int(hostport[1]))
        socket.setTimeout(timeout)
        socket = TAutoConnectSocket(socket)

        transport = TFramedTransport(socket)
        _base_protocol = TBinaryProtocol(transport)
    return _base_protocol


def get_base_protocol_4_pool(endpoint, timeout=2000):
    hostport = endpoint.split(":")
    socket = TSocket(hostport[0], int(hostport[1]))
    socket.setTimeout(timeout)
    socket = TAutoConnectSocket(socket)

    transport = TFramedTransport(socket)
    return TBinaryProtocol(transport)


def get_service_protocol(service, base_protocol=None):
    """
    多个不同的service可以共用一个base_protocol; 如果指定了service, 则在base_protocol的基础上添加一个新的wrap
    :param service:
    :param base_protocol:
    :return:
    """
    base_protocol = base_protocol or _base_protocol
    if service:
        return TMultiplexedProtocol(base_protocol, service)
    else:
        return base_protocol