package me.chunyu.rpc_proxy.server;

import org.apache.thrift.TByteArrayOutputStream;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class SelectAcceptThread extends Thread {
    protected final Logger LOGGER = LoggerFactory.getLogger(getClass().getName());

    protected final TNonblockingServerTransport serverTransport;
    protected final Selector selector;
    protected final Set<FrameBuffer> selectInterestChanges = new HashSet<FrameBuffer>();

    protected final RequestHandler handler;
    protected final AtomicBoolean stopped;

    protected final int maxReadBufferSize;

    public SelectAcceptThread(final TNonblockingServerTransport serverTransport, RequestHandler handler,
                              AtomicBoolean stopped, int maxReadBufferSize) throws IOException {

        this.serverTransport = serverTransport;

        // 创建selector, 并将serverTransport注册到selector上
        this.selector = SelectorProvider.provider().openSelector();
        this.serverTransport.registerSelector(this.selector);

        this.handler = handler;

        // 来自外部Server的 stop 状态变量
        this.stopped = stopped;
        this.maxReadBufferSize = maxReadBufferSize;
    }


    @Override
    public void run() {
        try {
            // 主循环，进行 event loop
            while (!stopped.get()) {
                select();
                processInterestChanges();
            }

            // 清理工作
            for (SelectionKey selectionKey : selector.keys()) {
                cleanupSelectionKey(selectionKey);
            }
        } catch (Throwable t) {
            LOGGER.error("run() exiting due to uncaught error", t);
        } finally {
            try {
                selector.close();
            } catch (IOException e) {
                LOGGER.error("Got an IOException while closing selector!", e);
            }
            stopped.set(true);
        }
    }


    /**
     * Select and process IO events appropriately:
     * If there are connections to be accepted, accept them. {@link #handleAccept()}
     * If there are existing connections with data waiting to be read, read it, {@link #handleRead(SelectionKey)}
     * buffering until a whole frame has been read.
     * If there are any pending responses, buffer them until their target client
     * is available, and then send the data.  {@link #handleWrite(SelectionKey)}
     */
    private void select() {
        try {
            // wait for io events.
            selector.select();

            Iterator<SelectionKey> selectedKeys = selector.selectedKeys().iterator();
            while (!stopped.get() && selectedKeys.hasNext()) {

                // 处理selector中存在事件的 keys
                SelectionKey key = selectedKeys.next();
                selectedKeys.remove();

                // skip if not valid(Connection关闭等等)
                if (!key.isValid()) {
                    cleanupSelectionKey(key);
                    continue;
                }


                if (key.isAcceptable()) {
                    // 接受新的Client
                    handleAccept();
                } else if (key.isReadable()) {
                    // 读取新的数据
                    handleRead(key);
                } else if (key.isWritable()) {
                    // 将数据写回Client
                    handleWrite(key);
                } else {
                    LOGGER.warn("Unexpected state in select! " + key.interestOps());
                }
            }
        } catch (IOException e) {
            LOGGER.warn("Got an IOException while selecting!", e);
        }
    }

    /**
     * 接受一个新的Client Connection
     *
     * @throws IOException
     */
    private void handleAccept() throws IOException {
        SelectionKey clientKey = null;
        TNonblockingTransport client = null;
        try {
            // accept the connection
            client = (TNonblockingTransport) serverTransport.accept();
            clientKey = client.registerSelector(selector, SelectionKey.OP_READ);

            // clientKey 和 Transport等对应关系，通过 clientKey的attach来实现的
            FrameBuffer frameBuffer = new FrameBuffer(client, clientKey, SelectAcceptThread.this,
                    maxReadBufferSize);
            clientKey.attach(frameBuffer);

        } catch (TTransportException tte) {
            LOGGER.warn("Exception trying to accept!", tte);
            tte.printStackTrace();
            if (clientKey != null) cleanupSelectionKey(clientKey);
            if (client != null) client.close();
        }
    }


    /**
     * Do the work required to read from a readable client. If the frame is
     * fully read, then invoke the method call.
     */
    protected void handleRead(SelectionKey key) {
        FrameBuffer buffer = (FrameBuffer) key.attachment();
        if (!buffer.read()) {
            cleanupSelectionKey(key);
            return;
        } else if (buffer.isFrameFullyRead()) {
            // 成功读取完毕一条记录之后，开始处理关键点:
            // buffer在此时的状态
            if (!this.handler.requestInvoke(buffer)) {
                cleanupSelectionKey(key);
            }
        }
    }

    /**
     * Let a writable client get written, if there's data to be written.
     */
    protected void handleWrite(SelectionKey key) {
        FrameBuffer buffer = (FrameBuffer) key.attachment();
        if (!buffer.write()) {
            cleanupSelectionKey(key);
        }
    }

    /**
     * 关闭connection, 且不再select
     */
    protected void cleanupSelectionKey(SelectionKey key) {
        FrameBuffer buffer = (FrameBuffer) key.attachment();
        if (buffer != null) {
            buffer.close();
        }
        key.cancel();
    }

    /**
     * 唤醒selector, 以便能修改被监听的 sockets
     */
    public void wakeupSelector() {
        selector.wakeup();
    }

    /**
     * Add FrameBuffer to the list of select interest changes and wake up the
     * selector if it's blocked. When the select() call exits, it'll give the
     * FrameBuffer a chance to change its interests.
     */
    public void requestSelectInterestChange(FrameBuffer frameBuffer) {
        synchronized (selectInterestChanges) {
            selectInterestChanges.add(frameBuffer);
        }
        // wakeup the selector, if it's currently blocked.
        selector.wakeup();
    }

    /**
     * Check to see if there are any FrameBuffers that have switched their
     * interest type from read to write or vice versa.
     */
    protected void processInterestChanges() {
        synchronized (selectInterestChanges) {
            for (FrameBuffer fb : selectInterestChanges) {
                fb.changeSelectInterests();
            }
            selectInterestChanges.clear();
        }
    }

    public boolean isStopped() {
        return stopped.get();
    }
}