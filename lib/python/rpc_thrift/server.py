# -*- coding: utf-8 -*-
from __future__ import absolute_import
from StringIO import StringIO
import os
import signal
import time

from colorama import Fore
import gevent
from rpc_thrift import MESSAGE_TYPE_HEART_BEAT
from thrift.transport.TSocket import TServerSocket
from thrift.transport.TTransport import TTransportException, TBufferedTransport

from rpc_thrift.config import print_exception
from rpc_thrift.protocol import TUtf8BinaryProtocol
from rpc_thrift.transport import TMemoryBuffer, TFramedTransportEx


class Server(object):
    def __init__(self, processor, address, pool_size=5, service=None):
        # 1. 获取zeromq context, 以及 events

        self.processor = processor  # thrift processor

        address = address.split(":")
        self.host = address[0]
        self.port = int(address[1])


        # 4. gevent
        self.task_pool = gevent.pool.Pool(size=pool_size)

        self.acceptor_task = None



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