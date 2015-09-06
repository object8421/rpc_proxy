package me.chunyu.rpc_proxy.server;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TNonblockingTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

public class FrameBuffer {
    private final Logger LOGGER = LoggerFactory.getLogger(getClass().getName());

    protected static final AtomicLong readBufferBytesAllocated = new AtomicLong(0);

    // Transport, 直接负责数据的读写
    protected final TNonblockingTransport trans;

    // 负责 selector transport相关的io
    protected final SelectionKey selectionKey;
    protected final SelectAcceptThread selectThread;

    // 读写是两个独立的过程(状态分离)
    protected FrameBufferState stateR = FrameBufferState.READING_FRAME_SIZE;
    protected FrameBufferState stateW = FrameBufferState.WRITING;

    // the ByteBuffer we'll be using to write and read, depending on the state
    protected ByteBuffer bufferR;
    protected ByteBuffer frameSizeW;
    protected ByteBuffer frameSizeR;

    protected ConcurrentLinkedDeque<ByteBuffer> buffersW;


    public void addWriteBuffer(ByteBuffer writeBuf, TException exp) {
        if (writeBuf != null) {
            buffersW.add(writeBuf);
            // 开启读写
            selectionKey.interestOps(SelectionKey.OP_WRITE | SelectionKey.OP_READ);

        } else if (exp != null) {
            stateW = FrameBufferState.AWAITING_CLOSE;
            requestSelectInterestChange();
        }


    }


    protected int maxReadBufferSize;

    public FrameBuffer(final TNonblockingTransport trans,
                       final SelectionKey selectionKey,
                       final SelectAcceptThread selectThread, int maxReadBufferSize) {
        // 管理数据的输入和输出
        this.trans = trans;
        this.selectionKey = selectionKey;
        this.selectThread = selectThread;

        buffersW = new ConcurrentLinkedDeque<ByteBuffer>();
        frameSizeR = ByteBuffer.allocate(4);
        frameSizeW = ByteBuffer.allocate(4);

        this.maxReadBufferSize = maxReadBufferSize;

    }

    /**
     * Check if this FrameBuffer has a full frame read.
     */
    public boolean isFrameFullyRead() {
        return stateR == FrameBufferState.READ_FRAME_COMPLETE;
    }

    /**
     * Give this FrameBuffer a chance to read. The selector loop should have
     * received a read event for this FrameBuffer.
     *
     * @return true if the connection should live on, false if it should be
     * closed
     * 两种状态下: 才能判断一个Frame是否读取完毕: {#link #read() }
     * read()返回true, 表示数据读取正常, {@link #isFrameFullyRead()}
     */
    public boolean read() {
        // 状态切换:
        // stateR
        //         READING_FRAME_SIZE ---> READING_FRAME ---> READ_FRAME_COMPLETE --->

        // 由于TCP包是整体到达的，因此Frame Size会一次性读取成功，但是: 后面的部分不能保证
        if (stateR == FrameBufferState.READING_FRAME_SIZE || stateR == FrameBufferState.READ_FRAME_COMPLETE) {

            frameSizeR.clear();

            if (!internalRead(frameSizeR)) {
                return false;
            }

            // if the frame size has been read completely, then prepare to read the
            // actual frame.
            if (frameSizeR.remaining() == 0) {
                // pull out the frame size as an integer.
                int frameSize = frameSizeR.getInt(0);
                if (frameSize <= 0) {
                    LOGGER.error("Read an invalid frame size of " + frameSize
                            + ". Are you using TFramedTransport on the client side?");
                    return false;
                }

                // if this frame will always be too large for this server, log the
                // error and close the connection.
                // 直接关闭connection
                if (frameSize > maxReadBufferSize) {
                    LOGGER.error("Read a frame size of " + frameSize
                            + ", which is bigger than the maximum allowable buffer size for ALL connections.");
                    return false;
                }

                // if this frame will push us over the memory limit, then return.
                // with luck, more memory will free up the next time around.
                if (readBufferBytesAllocated.get() + frameSize > maxReadBufferSize) {
                    return true;
                }

                // increment the amount of memory allocated to read buffers
                readBufferBytesAllocated.addAndGet(frameSize);

                // reallocate the readbuffer as a frame-sized buffer
                // TODO: 每次来一个新的包，都会重新申请内存
                bufferR = ByteBuffer.allocate(frameSize + 4);
                bufferR.putInt(frameSize);

                LOGGER.info("Message Frame Size: " + frameSize);

                stateR = FrameBufferState.READING_FRAME;
            } else {
                // this skips the check of READING_FRAME state below, since we can't
                // possibly go on to that state if there's data left to be read at
                // this one.
                return true;
            }
        }

        // it is possible to fall through from the READING_FRAME_SIZE section
        // to READING_FRAME if there's already some frame data available once
        // READING_FRAME_SIZE is complete.

        if (stateR == FrameBufferState.READING_FRAME) {
            if (!internalRead(bufferR)) {
                return false;
            }

            // since we're already in the select loop here for sure, we can just
            // modify our selection key directly.
            if (bufferR.remaining() == 0) {
                // get rid of the read select interests
                stateR = FrameBufferState.READ_FRAME_COMPLETE;
            }

            return true;
        }

        // if we fall through to this point, then the state must be invalid.
        LOGGER.error("Read was called but state is invalid (" + stateR + ")");
        return false;
    }

    public ByteBuffer getBufferR() {
        return bufferR;
    }

    /**
     * Give this FrameBuffer a chance to write its output to the final client.
     */
    public boolean write() {
        if (stateW == FrameBufferState.WRITING) {
            ByteBuffer bufferW = buffersW.peek();
            if (bufferW != null) {
                try {
                    // 可以考虑将多个bufferW合并放在一个 tcp package中，减少系统调用
                    int n;
                    if ((n = trans.write(bufferW)) < 0) {
                        return false;
                    } else {

                        byte[] response = bufferW.array();
                        final TMemoryInputTransport frameTrans = new TMemoryInputTransport(response, 4, response.length - 4);
                        final TBinaryProtocol in = new TBinaryProtocol(frameTrans);
                        try {
                            TMessage msg = in.readMessageBegin();
                            LOGGER.info(String.format("Write Back: %s, %d,Seq: %d", msg.name, msg.type, msg.seqid));
                        } catch (Exception e) {

                        }


                        LOGGER.info("----> Write Frame Body: " + n);
                    }
                } catch (IOException e) {
                    LOGGER.warn("Got an IOException during write!", e);
                    return false;
                }

                // 如果当前的Node已经处理完毕，则直接输出
                if (!bufferW.hasRemaining()) {
                    buffersW.pop();
                }

            } else {
                // 如果没有数据输出，则暂停Write(下一轮就能看到效果)
                selectionKey.interestOps(SelectionKey.OP_READ);
            }
            return true;
        }

        LOGGER.error("Write was called, but state is invalid (" + stateW + ")");
        return false;
    }

    /**
     * Give this FrameBuffer a chance to set its interest to write, once data
     * has come in.
     */
    public void changeSelectInterests() {
        // 状态迁移(现在似乎没有什么要迁移的)
        if (stateW == FrameBufferState.AWAITING_CLOSE) {
            close();
            selectionKey.cancel();
        }
    }

    /**
     * Shut the connection down.
     */
    public void close() {
        // if we're being closed due to an error, we might have allocated a
        // buffer that we need to subtract for our memory accounting.
        if (bufferR != null) {
            readBufferBytesAllocated.addAndGet(-bufferR.array().length);
        }
        trans.close();
    }


    /**
     * Perform a read into buffer.
     *
     * @return true if the read succeeded, false if there was an error or the
     * connection closed.
     */
    private boolean internalRead(ByteBuffer buff) {
        try {
            if (trans.read(buff) < 0) {
                return false;
            }
            return true;
        } catch (IOException e) {
            LOGGER.warn("Got an IOException in internalRead!", e);
            return false;
        }
    }

    /**
     * When this FrameBuffer needs to change its select interests and execution
     * might not be in its select thread, then this method will make sure the
     * interest change gets done when the select thread wakes back up. When the
     * current thread is this FrameBuffer's select thread, then it just does the
     * interest change immediately.
     */
    protected void requestSelectInterestChange() {
        if (Thread.currentThread() == this.selectThread) {
            changeSelectInterests();
        } else {
            this.selectThread.requestSelectInterestChange(this);
        }
    }
} // FrameBuffer