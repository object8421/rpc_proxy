# -*- coding: utf-8 -*-
from __future__ import absolute_import
from StringIO import StringIO
from struct import pack, unpack

from thrift.transport.TTransport import TTransportBase, TTransportException


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

class TAutoConnectFramedTransport(TTransportBase):
    """
        将socket进行包装，提供了自动重连的功能, 重连之后清空之前的状态
    """
    def __init__(self, socket):
        self.socket = socket

        self.wbuf = StringIO()
        self.rbuf = StringIO()

    def isOpen(self):
        return self.socket.isOpen()

    def open(self):
        """
            open之后需要重置状态
        """
        self.socket.open()

        # 恢复状态
        self.reset_buff()

    def close(self):
        self.socket.close()

    def reset_buff(self):
        if self.wbuf.len != 0:
            self.wbuf = StringIO()
        if self.rbuf.len != 0:
            self.rbuf = StringIO()

    def read(self, sz):
        if not self.isOpen():
            self.open()

        ret = self.rbuf.read(sz)
        if len(ret) != 0:
            return ret

        try:
            self.__readFrame()
            return self.rbuf.read(sz)
        except Exception: # TTransportException, timeout, Broken Pipe
            self.close()
            raise

    def __readFrame(self):
        buff = self.socket.readAll(4)
        sz, = unpack('!i', buff)
        self.rbuf = StringIO(self.socket.readAll(sz))


    def write(self, buf):
        if not self.isOpen():
            self.open()
        self.wbuf.write(buf)

    def flush(self):
        if not self.isOpen():
            self.open()

        wout = self.wbuf.getvalue()
        wsz = len(wout)
        self.wbuf = StringIO() # 状态恢复了

        # print "TFramedTransport#Flush Frame Size: ", wsz
        try:
            # 首先写入长度，并且Flush之前的数据
            self.socket.write(pack("!i", wsz))
            self.socket.write(wout)
            self.socket.flush()
        except Exception:
            self.close()
            raise


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
