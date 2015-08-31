# -*- coding: utf-8 -*-
from __future__ import absolute_import
from logging import getLogger
import os
from StringIO import StringIO
import signal
from struct import unpack, pack
import traceback
import sys

import gevent.pool
import gevent.queue
import gevent.event
import gevent.local
import gevent.lock
from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.transport.TSocket import TSocket, TServerSocket
from thrift.transport.TTransport import TTransportBase, TBufferedTransport, TTransportException
import time

MESSAGE_TYPE_HEART_BEAT = 20
MESSAGE_TYPE_STOP = 21


logger = getLogger(__name__)

from colorama import init
init()
from colorama import Fore


class TUtf8BinaryProtocol(TBinaryProtocol):
    """
        只要控制数据的输出端，在整个transport上的数据都应该是标准的
    """
    def writeString(self, str):
        if isinstance(str, unicode):
            str = str.encode("utf-8")

        TBinaryProtocol.writeString(self, str)


class Server(object):
    def __init__(self, processor, address, pool_size=5, service=None, worker_mode=True):
        # 1. 获取zeromq context, 以及 events

        self.processor = processor  # thrift processor

        address = address.split(":")
        self.host = address[0]
        self.port = int(address[1])


        # 4. gevent
        self.task_pool = gevent.pool.Pool(size=pool_size)

        self.acceptor_task = None

        self.worker_mode = worker_mode


        # 5. 程序退出控制
        self.alive = True
        self.t = 0
        self.count = 0

        self.reconnect_interval = 1

        self.responses = []

        self.queue = None
        self.socket = None

    def handle_request(self, proto_input, queue):

        trans_output = TMemoryBuffer()
        proto_output = TUtf8BinaryProtocol(trans_output)

        # 2. 交给processor来处理
        try:
            # print "Begin process"
            self.processor.process(proto_input, proto_output)
            # 3. 将thirft的结果转换成为 zeromq 格式的数据
            msg = trans_output.getvalue()

            queue.put(msg)

        except Exception as e:
            # 如何出现了异常该如何处理呢
            # 程序不能挂
            print_exception()
            # 如何返回呢?


    def loop_all(self):
        if self.worker_mode:
            while self.alive:
                # 创建一个到lb的连接，然后开始读取Frame, 并且返回数据
                print "Prepare open a socket to lb: %s:%d" % (self.host, self.port)
                try:
                    socket = TSocket(host=self.host, port=self.port)
                    socket.open()
                except TTransportException:
                    print "Sleep %ds for another retry" % self.reconnect_interval
                    time.sleep(self.reconnect_interval)

                    if self.reconnect_interval < 4:
                        self.reconnect_interval *= 2
                    continue

                self.reconnect_interval = 1
                self.socket = socket

                trans = TFramedTransportEx(TBufferedTransport(socket))
                self.queue = gevent.queue.Queue()
                #
                # print "Start read/write loop on trans"
                #
                # # 如果是worker mode, 则先读取，后写入
                g1=gevent.spawn(self.loop_reader, trans, self.queue)
                g2=gevent.spawn(self.loop_writer, trans, self.queue)
                gevent.joinall([g1, g2])


                try:
                    # print "Trans Closed, queue size: ", self.queue.qsize()
                    self.queue = None
                    self.socket.close()
                    self.socket = None
                    trans.close()

                except:
                    print_exception()
                    pass

                time.sleep(1)



        else:
            # 日常测试(Client端存在严格的时序)
            socket = TServerSocket(host=self.host, port=self.port)
            socket.open()
            socket.listen()

            while True:
                tsocket = socket.accept()
                print "Get A Connection: ", tsocket


                # 如果出现None, 则表示要结束了
                queue = gevent.queue.Queue()
                trans = TFramedTransportEx(TBufferedTransport(tsocket))
                gevent.spawn(self.loop_reader, trans, queue)
                gevent.spawn(self.loop_writer, trans, queue)

    def loop_reader(self, trans, queue):
        """
        :param tsocket:
        :param queue:
        :return:
        """
        """
        :param tsocket:
        :param queue:
        :return:
        """
        last_hb_time = time.time()

        while self.alive:
            # 启动就读取数据
            # 什么时候知道是否还有数据呢?
            try:
                # 预先解码数据
                frame = trans.readFrameEx()


                frameIO = StringIO(frame)
                trans_input = TMemoryBuffer(frameIO)
                proto_input = TUtf8BinaryProtocol(trans_input)
                name, type, seqid = proto_input.readMessageBegin()
                frameIO.seek(0) # 将proto_input复原

                # 如果是信条，则直接返回
                if type == MESSAGE_TYPE_HEART_BEAT:
                    queue.put(frame)
                    last_hb_time = time.time()
                    # print "Received Heartbeat Signal........"
                    continue
                else:
                    # print "----->Frame", frame
                    self.task_pool.spawn(self.handle_request, proto_input, queue)
                    # self.handle_request(frame, queue)
            except TTransportException as e:
                # EOF是很正常的现象
                if e.type != TTransportException.END_OF_FILE:
                    print_exception()
                print "....Worker Connection To LB Failed, LoopWrite Stop"
                queue.put(None) # 表示要结束了
                break
            except:
                print_exception()
                queue.put(None) # 表示要结束了
                break



    def loop_writer(self, trans, queue):
        """
        异步写入数据
        :param trans:
        :param queue:
        :return:
        """
        msg = queue.get()
        while msg is not None:
            try:
                # print "====> ", msg
                trans.write(msg)
                trans.flush()
            except:
                print_exception()
                break

            # 简单处理
            if not self.alive:
                break
            msg = queue.get()
        if msg is None:
            print "....Worker Connection To LB Failed, LoopRead Stop"




    def run(self):
        import gevent.monkey
        gevent.monkey.patch_socket()

        # 0. 注册信号(控制运维)
        self.init_signal()

        # 2. 监听数据
        # self.loop_reader()
        self.acceptor_task = gevent.spawn(self.loop_all)

        # 3. 等待结束
        try:
            self.acceptor_task.get()
        finally:
            self.stop()
            self.task_pool.join(raise_error=True)

    def stop(self):
        if self.acceptor_task is not None:
            self.acceptor_task.kill()
            self.acceptor_task = None

    def init_signal(self):
        def handle_int(*_):
            print Fore.RED, "Receive Exit Signal", Fore.RESET
            self.alive = False

            if self.queue:
                self.queue.put(None)

            if self.socket:
                self.socket.close()


        def handle_term(*_):
            # 主动退出
            print Fore.RED, "Receive Exit Signal", Fore.RESET
            self.alive = False
            if self.queue:
                self.queue.put(None)
            if self.socket:
                self.socket.close()

        # 2/15
        signal.signal(signal.SIGINT, handle_int)
        signal.signal(signal.SIGTERM, handle_term)

        print Fore.RED, "To graceful stop current worker plz. use:", Fore.RESET
        print Fore.GREEN, ("kill -15 %s" % os.getpid()), Fore.RESET


class TFramedTransportEx(TTransportBase):
    # """Class that wraps another transport and frames its I/O when writing."""
    #
    def __init__(self, trans, ):
        self.trans = trans
        self.wbuf = StringIO()
        self.rbuf = StringIO()

    def isOpen(self):
        return self.trans.isOpen()

    def open(self):
        return self.trans.open()

    def close(self):
        return self.trans.close()


    def read(self, sz):
        # print "Read Size: ", sz
        ret = self.rbuf.read(sz)
        if len(ret) != 0:
            return ret

        self.readFrame()
        return self.rbuf.read(sz)


    def readFrame(self):


        buff = self.trans.readAll(4)
        sz, = unpack('!i', buff)
        # print "readFrame: ", sz

        self.rbuf = StringIO(self.trans.readAll(sz))


    def readFrameEx(self):
        """
            直接以bytes的形式返回一个Frame
        """
        buff = self.trans.readAll(4)
        sz, = unpack('!i', buff)

        bytes = self.trans.readAll(sz)
        return bytes

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

class TStringBuffer(TTransportBase):
    """
        只读
    """
    def __init__(self, value):
        assert isinstance(value, basestring)
        self._buffer = value
        self._offset = 0
        self._len = len(value)

    def isOpen(self):
        return True

    def open(self):
        pass

    def close(self):
        self._offset = 0

    def read(self, sz):
        result = self._buffer[self._offset:(self._offset + sz)]
        self._offset = self._offset + sz
        return result

    def write(self, buf):
        raise Exception("Not Implemented")

    def flush(self):
        pass

    def getvalue(self):
        return self._buffer

# 配置文件相关的参数
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
