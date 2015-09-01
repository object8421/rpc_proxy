# -*- coding: utf-8 -*-
from __future__ import absolute_import
from rpc_thrift import MESSAGE_TYPE_STOP
from rpc_thrift.protocol import TUtf8BinaryProtocol
from rpc_thrift.transport import TMemoryBuffer


def new_rpc_exit_message():
    buf = TMemoryBuffer()
    protocol = TUtf8BinaryProtocol(buf)

    protocol.writeMessageBegin("worker_shutdonw", MESSAGE_TYPE_STOP, 0)
    protocol.writeMessageEnd()
    return buf.getvalue()
