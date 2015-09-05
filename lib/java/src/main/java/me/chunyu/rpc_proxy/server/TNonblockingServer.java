package me.chunyu.rpc_proxy.server;


import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;


/**
 * 一个线程负责专门的I/O
 * 其他的线程负责处理具体的请求
 */
public class TNonblockingServer implements RequestHandler {
    protected final Logger LOGGER = LoggerFactory.getLogger(getClass().getName());

    // 控制client最大允许分配的内存(如果不控制，很容易就OOM)
    final long MAX_READ_BUFFER_BYTES;

    /**
     * How many bytes are currently allocated to read buffers.
     */
    final AtomicLong readBufferBytesAllocated = new AtomicLong(0);
    private boolean isServing;
    protected TServerEventHandler eventHandler_;
    protected TServerTransport serverTransport_;

    private final ExecutorService invoker;
    private final int stopTimeoutVal;

    // int workerThreads = 5, int stopTimeoutVal = 60, TimeUnit stopTimeoutUnit = TimeUnit.SECONDS, ExecutorService executorService = null
    public TNonblockingServer(int workerThreads, int stopTimeoutVal) {
        MAX_READ_BUFFER_BYTES = 64 * 1024 * 1024;

        this.stopTimeoutVal = stopTimeoutVal;
        LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();
        invoker = new ThreadPoolExecutor(workerThreads, workerThreads, stopTimeoutVal, TimeUnit.SECONDS, queue);
    }

    public boolean isServing() {
        return this.isServing;
    }

    protected void setServing(boolean serving) {
        this.isServing = serving;
    }

    /**
     * Begin accepting connections and processing invocations.
     */
    public void serve() {
        // start any IO threads
        if (!startThreads()) {
            return;
        }

        // start listening, or exit
        if (!startListening()) {
            return;
        }

        setServing(true);

        // this will block while we serve
        waitForShutdown();

        setServing(false);

        // do a little cleanup
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
            serverTransport_.listen();
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
        serverTransport_.close();
    }


    // Flag for stopping the server
    // Please see THRIFT-1795 for the usage of this flag
    private AtomicBoolean stopped_ = new AtomicBoolean(false);


    private SelectAcceptThread selectAcceptThread_;

    protected boolean startThreads() {
        // start the selector
        try {
            selectAcceptThread_ = new SelectAcceptThread((TNonblockingServerTransport) serverTransport_, this, this.stopped_);
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

//    public boolean requestInvoke(FrameBuffer frameBuffer) {
//        frameBuffer.invoke();
//        return true;
//    }


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
    public boolean requestInvoke(FrameBuffer frameBuffer) {
        try {
            invoker.execute(new Runnable() {
                @Override
                public void run() {
                    // frameBuffer
                    // 如何异步地处理的frameBuffer呢?
                }
            });
            return true;
        } catch (RejectedExecutionException rx) {
            LOGGER.warn("ExecutorService rejected execution!", rx);
            return false;
        }
    }

}