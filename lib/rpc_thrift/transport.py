# -*- coding: utf-8 -*-
from __future__ import absolute_import
from StringIO import StringIO
from struct import pack, unpack
from thrift.transport.TTransport import TTransportBase


class TFramedTransportEx(TTransportBase):
    """
        和 TFramedTransport的区别:
            多了一个 readFrameEx 函数, 由于 TFramedTransport 中的变量 __wbuf, __rbuf不能直接访问，因此就重写了该类
    """
    def __init__(self, trans, ):
        self.trans = trans
        self.wbuf = StringIO()
        self.rbuf = StringIO()

    def readFrameEx(self):
        """
            直接以bytes的形式返回一个Frame
        """
        buff = self.trans.readAll(4)
        sz, = unpack('!i', buff)

        bytes = self.trans.readAll(sz)
        return bytes


    def isOpen(self):
        return self.trans.isOpen()

    def open(self):
        return self.trans.open()

    def close(self):
        return self.trans.close()


    def read(self, sz):
        ret = self.rbuf.read(sz)
        if len(ret) != 0:
            return ret

        self.readFrame()
        return self.rbuf.read(sz)


    def readFrame(self):
        buff = self.trans.readAll(4)
        sz, = unpack('!i', buff)
        self.rbuf = StringIO(self.trans.readAll(sz))


    def write(self, buf):
        self.wbuf.write(buf)

    def flush(self):
        wout = self.wbuf.getvalue()
        wsz = len(wout)
        self.wbuf = StringIO()

        # print "TFramedTransport#Flush Frame Size: ", wsz

        # 首先写入长度，并且Flush之前的数据
        self.trans.write(pack("!i", wsz))
        self.trans.write(wout)
        self.trans.flush()


class TMemoryBuffer(TTransportBase):
    def __init__(self, value=None):
        if value is not None:
            if isinstance(value, StringIO):
                self._buffer = value
            else:
                self._buffer = StringIO(value)
        else:
            self._buffer = StringIO()

    def isOpen(self):
        return not self._buffer.closed

    def open(self):
        pass

    def close(self):
        self._buffer.close()

    def read(self, sz):
        return self._buffer.read(sz)

    def write(self, buf):
        self._buffer.write(buf)

    def flush(self):
        pass

    def getvalue(self):
        return self._buffer.getvalue()
