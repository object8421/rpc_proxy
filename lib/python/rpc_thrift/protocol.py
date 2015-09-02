# -*- coding: utf-8 -*-
from __future__ import absolute_import
from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.Thrift import TMessageType
from thrift.protocol import TProtocolDecorator
import time


class TUtf8BinaryProtocol(TBinaryProtocol):
    def writeString(self, v):
        """
            只要控制好了writeString, 在整个thrift系统中，所有的字符串都是utf-8格式的
        """
        if isinstance(v, unicode):
            v = v.encode("utf-8")

        # TBinaryProtocol 为 old style class
        TBinaryProtocol.writeString(self, v)


SEPARATOR = ":"


class TLoggerMultiplexedProtocol(TProtocolDecorator.TProtocolDecorator):
    def __init__(self, protocol, serviceName, logger):
        TProtocolDecorator.TProtocolDecorator.__init__(self, protocol)
        self.serviceName = serviceName
        self.logger = logger
        self.start = None
        self.last_name = None

    def writeMessageBegin(self, name, type, seqid):

        self.start = time.time()
        self.last_name = self.serviceName + SEPARATOR + name
        self.logger.info("\033[35m[RPC] %s\033[39m[%s] start", self.last_name, seqid) # 为了减少字符串操作，直接将Fore.MAGENTA 硬编码到文件中

        if (type == TMessageType.CALL or type == TMessageType.ONEWAY):
            self.protocol.writeMessageBegin(self.last_name, type, seqid)
        else:
            self.protocol.writeMessageBegin(name, type, seqid)


    def readMessageBegin(self):
        name, type, seqid = self.protocol.readMessageBegin()
        elapsed = (time.time() - self.start) * 1000

        self.logger.info("\033[35m[RPC] %s\033[39m[%s] ends, Elapsed: %.3fms", self.last_name, seqid, elapsed)
        return name, type, seqid



