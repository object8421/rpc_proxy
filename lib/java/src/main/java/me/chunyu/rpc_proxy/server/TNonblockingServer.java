package me.chunyu.rpc_proxy.server;


import org.apache.thrift.TByteArrayOutputStream;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.transport.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * 一个线程负责专门的I/O
 * 其他的线程负责处理具体的请求
 */
public class TNonblockingServer implements RequestHandler {
    protected final Logger LOGGER = LoggerFactory.getLogger(getClass().getName());


    // 不能使用负数
    final byte MESSAGE_TYPE_HEART_BEAT = 20;
    final byte MESSAGE_TYPE_STOP = 21;


    // 控制client最大允许分配的内存(如果不控制，很容易就OOM)
    final int MAX_READ_BUFFER_BYTES;

    /**
     * 是否正在对外启动服务
     */
    private boolean isServing;

    protected TServerTransport serverTransport;

    private ExecutorService invoker;
    private int stopTimeoutVal;

    private final TProcessor processor;

    // int workerThreads = 5, int stopTimeoutVal = 60, TimeUnit stopTimeoutUnit = TimeUnit.SECONDS, ExecutorService executorService = null
    public TNonblockingServer(TProcessor processor) {
        MAX_READ_BUFFER_BYTES = 64 * 1024 * 1024;


        this.processor = processor;


    }

    public TServerTransport getServerTransport() {
        return serverTransport;
    }

    public void setServerTransport(TServerTransport serverTransport) {
        this.serverTransport = serverTransport;
    }

    public void setUp(int workerThreads, int stopTimeoutVal) {
        this.stopTimeoutVal = stopTimeoutVal;
        LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();
        this.invoker = new ThreadPoolExecutor(workerThreads, workerThreads, stopTimeoutVal,
                TimeUnit.SECONDS, queue);
    }

    public void serve() {
        // 1. 启动 I/O Threads
        if (!startThreads()) {
            return;
        }

        // 2. 监听端口
        if (!startListening()) {
            return;
        }

        // 3.  标记服务开始运行
        //     可以配合zk进行服务注册
        setServing(true);

        // 4. 等待服务结束
        waitForShutdown();

        // 5. 下线服务
        setServing(false);

        // 6. 停止Listening
        stopListening();
    }


    /**
     * Have the server transport start accepting connections.
     *
     * @return true if we started listening successfully, false if something went
     * wrong.
     */
    protected boolean startListening() {
        try {
            serverTransport.listen();
            return true;
        } catch (TTransportException ttx) {
            LOGGER.error("Failed to start listening on server socket!", ttx);
            return false;
        }
    }

    /**
     * Stop listening for connections.
     */
    protected void stopListening() {
        serverTransport.close();
    }


    // Flag for stopping the server
    // Please see THRIFT-1795 for the usage of this flag
    private AtomicBoolean stopped_ = new AtomicBoolean(false);


    private SelectAcceptThread selectAcceptThread_;

    protected boolean startThreads() {
        // start the selector
        try {
            selectAcceptThread_ = new SelectAcceptThread((TNonblockingServerTransport) serverTransport,
                    this, this.stopped_, MAX_READ_BUFFER_BYTES);
            selectAcceptThread_.start();
            return true;
        } catch (IOException e) {
            LOGGER.error("Failed to start selector thread!", e);
            return false;
        }
    }


    protected void waitForShutdown() {
        joinSelector();
        gracefullyShutdownInvokerPool();
    }

    /**
     * Block until the selector thread exits.
     */
    protected void joinSelector() {
        // wait until the selector thread exits
        try {
            selectAcceptThread_.join();
        } catch (InterruptedException e) {
            // for now, just silently ignore. technically this means we'll have less of
            // a graceful shutdown as a result.
        }
    }

    /**
     * Stop serving and shut everything down.
     */
    public void stop() {
        stopped_.set(true);
        if (selectAcceptThread_ != null) {
            selectAcceptThread_.wakeupSelector();
        }
    }


    public boolean isStopped() {
        return stopped_.get();
    }


    protected void gracefullyShutdownInvokerPool() {
        // try to gracefully shut down the executor service
        invoker.shutdown();

        // Loop until awaitTermination finally does return without a interrupted
        // exception. If we don't do this, then we'll shut down prematurely. We want
        // to let the executorService clear it's task queue, closing client sockets
        // appropriately.
        long timeoutMS = TimeUnit.SECONDS.toMillis(stopTimeoutVal);
        long now = System.currentTimeMillis();
        while (timeoutMS >= 0) {
            try {
                invoker.awaitTermination(timeoutMS, TimeUnit.MILLISECONDS);
                break;
            } catch (InterruptedException ix) {
                long newnow = System.currentTimeMillis();
                timeoutMS -= (newnow - now);
                now = newnow;
            }
        }
    }


    // 异步地处理请求
    @Override
    public boolean requestInvoke(final FrameBuffer frameBuffer) {
        try {
            // 第一件事情: 读取当前的Request, 否则
            final byte[] request = frameBuffer.getBufferR().array();


            final TMemoryInputTransport frameTrans = new TMemoryInputTransport(request);
            final TByteArrayOutputStream response = new TByteArrayOutputStream();
            final TBinaryProtocol in = new TBinaryProtocol(frameTrans);
            final TBinaryProtocol out = new TBinaryProtocol(new TIOStreamTransport(response));

            try {
                TMessage msg = in.readMessageBegin();
                if (msg.type == MESSAGE_TYPE_HEART_BEAT) {
                    LOGGER.info("GOT HEART_BEAT MESSAGE.....");
                    ByteBuffer writeBuf = ByteBuffer.wrap(request);
                    frameBuffer.addWriteBuffer(writeBuf, null);
                    return true;
                } else {
                    frameTrans.reset(request);
                }
            } catch (TException e) {
                frameBuffer.addWriteBuffer(null, e);
                return false;
            }

            invoker.execute(new Runnable() {
                @Override
                public void run() {

                    try {
                        processor.process(in, out);
                        ByteBuffer writeBuf = ByteBuffer.wrap(response.get(), 0, response.len());

                        frameBuffer.addWriteBuffer(writeBuf, null);


                    } catch (TException e) {
                        LOGGER.warn("Exception Found: ", e);
                        frameBuffer.addWriteBuffer(null, e);
                    }
                }
            });
            return true;
        } catch (RejectedExecutionException rx) {
            LOGGER.warn("ExecutorService rejected execution!", rx);
            return false;
        }
    }

    public boolean isServing() {
        return this.isServing;
    }

    protected void setServing(boolean serving) {
        this.isServing = serving;
    }

}