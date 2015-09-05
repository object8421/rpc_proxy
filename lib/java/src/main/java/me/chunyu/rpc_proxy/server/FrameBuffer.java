package me.chunyu.rpc_proxy.server;

import org.apache.thrift.TByteArrayOutputStream;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

public class FrameBuffer {
    private final Logger LOGGER = LoggerFactory.getLogger(getClass().getName());

    // the actual transport hooked up to the client.
    protected final TNonblockingTransport trans_;

    // the SelectionKey that corresponds to our transport
    protected final SelectionKey selectionKey_;

    // the SelectThread that owns the registration of our transport
    protected final SelectAcceptThread selectThread_;

    // where in the process of reading/writing are we?
    protected FrameBufferState state_ = FrameBufferState.READING_FRAME_SIZE;

    // the ByteBuffer we'll be using to write and read, depending on the state
    protected ByteBuffer buffer_;

    protected final TByteArrayOutputStream response_;

    // the frame that the TTransport should wrap.
    protected final TMemoryInputTransport frameTrans_;

    // the transport that should be used to connect to clients
    protected final TTransport inTrans_;

    protected final TTransport outTrans_;

    // the input protocol to use on frames
    protected final TProtocol inProt_;

    // the output protocol to use on frames
    protected final TProtocol outProt_;

    // context associated with this connection
    protected final ServerContext context_;

    public FrameBuffer(final TNonblockingTransport trans,
                       final SelectionKey selectionKey,
                       final SelectAcceptThread selectThread) {
        trans_ = trans;
        selectionKey_ = selectionKey;
        selectThread_ = selectThread;
        buffer_ = ByteBuffer.allocate(4);

        frameTrans_ = new TMemoryInputTransport();
        response_ = new TByteArrayOutputStream();
        inTrans_ = inputTransportFactory_.getTransport(frameTrans_);
        outTrans_ = outputTransportFactory_.getTransport(new TIOStreamTransport(response_));
        inProt_ = inputProtocolFactory_.getProtocol(inTrans_);
        outProt_ = new TBinaryProtocol(response_);


        if (eventHandler_ != null) {
            context_ = eventHandler_.createContext(inProt_, outProt_);
        } else {
            context_ = null;
        }
    }

    /**
     * Give this FrameBuffer a chance to read. The selector loop should have
     * received a read event for this FrameBuffer.
     *
     * @return true if the connection should live on, false if it should be
     * closed
     */
    public boolean read() {
        if (state_ == FrameBufferState.READING_FRAME_SIZE) {
            // try to read the frame size completely
            if (!internalRead()) {
                return false;
            }

            // if the frame size has been read completely, then prepare to read the
            // actual frame.
            if (buffer_.remaining() == 0) {
                // pull out the frame size as an integer.
                int frameSize = buffer_.getInt(0);
                if (frameSize <= 0) {
                    LOGGER.error("Read an invalid frame size of " + frameSize
                            + ". Are you using TFramedTransport on the client side?");
                    return false;
                }

                // if this frame will always be too large for this server, log the
                // error and close the connection.
                if (frameSize > MAX_READ_BUFFER_BYTES) {
                    LOGGER.error("Read a frame size of " + frameSize
                            + ", which is bigger than the maximum allowable buffer size for ALL connections.");
                    return false;
                }

                // if this frame will push us over the memory limit, then return.
                // with luck, more memory will free up the next time around.
                if (readBufferBytesAllocated.get() + frameSize > MAX_READ_BUFFER_BYTES) {
                    return true;
                }

                // increment the amount of memory allocated to read buffers
                readBufferBytesAllocated.addAndGet(frameSize + 4);

                // reallocate the readbuffer as a frame-sized buffer
                buffer_ = ByteBuffer.allocate(frameSize + 4);
                buffer_.putInt(frameSize);

                state_ = FrameBufferState.READING_FRAME;
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

        if (state_ == FrameBufferState.READING_FRAME) {
            if (!internalRead()) {
                return false;
            }

            // since we're already in the select loop here for sure, we can just
            // modify our selection key directly.
            if (buffer_.remaining() == 0) {
                // get rid of the read select interests
                selectionKey_.interestOps(0);
                state_ = FrameBufferState.READ_FRAME_COMPLETE;
            }

            return true;
        }

        // if we fall through to this point, then the state must be invalid.
        LOGGER.error("Read was called but state is invalid (" + state_ + ")");
        return false;
    }

    /**
     * Give this FrameBuffer a chance to write its output to the final client.
     */
    public boolean write() {
        if (state_ == FrameBufferState.WRITING) {
            try {
                if (trans_.write(buffer_) < 0) {
                    return false;
                }
            } catch (IOException e) {
                LOGGER.warn("Got an IOException during write!", e);
                return false;
            }

            // we're done writing. now we need to switch back to reading.
            if (buffer_.remaining() == 0) {
                prepareRead();
            }
            return true;
        }

        LOGGER.error("Write was called, but state is invalid (" + state_ + ")");
        return false;
    }

    /**
     * Give this FrameBuffer a chance to set its interest to write, once data
     * has come in.
     */
    public void changeSelectInterests() {
        if (state_ == FrameBufferState.AWAITING_REGISTER_WRITE) {
            // set the OP_WRITE interest
            selectionKey_.interestOps(SelectionKey.OP_WRITE);
            state_ = FrameBufferState.WRITING;
        } else if (state_ == FrameBufferState.AWAITING_REGISTER_READ) {
            prepareRead();
        } else if (state_ == FrameBufferState.AWAITING_CLOSE) {
            close();
            selectionKey_.cancel();
        } else {
            LOGGER.error("changeSelectInterest was called, but state is invalid (" + state_ + ")");
        }
    }

    /**
     * Shut the connection down.
     */
    public void close() {
        // if we're being closed due to an error, we might have allocated a
        // buffer that we need to subtract for our memory accounting.
        if (state_ == FrameBufferState.READING_FRAME ||
                state_ == FrameBufferState.READ_FRAME_COMPLETE ||
                state_ == FrameBufferState.AWAITING_CLOSE) {
            readBufferBytesAllocated.addAndGet(-buffer_.array().length);
        }
        trans_.close();
        if (eventHandler_ != null) {
            eventHandler_.deleteContext(context_, inProt_, outProt_);
        }
    }

    /**
     * Check if this FrameBuffer has a full frame read.
     */
    public boolean isFrameFullyRead() {
        return state_ == FrameBufferState.READ_FRAME_COMPLETE;
    }

    /**
     * After the processor has processed the invocation, whatever thread is
     * managing invocations should call this method on this FrameBuffer so we
     * know it's time to start trying to write again. Also, if it turns out that
     * there actually isn't any data in the response buffer, we'll skip trying
     * to write and instead go back to reading.
     */
    public void responseReady() {
        // the read buffer is definitely no longer in use, so we will decrement
        // our read buffer count. we do this here as well as in close because
        // we'd like to free this read memory up as quickly as possible for other
        // clients.
        readBufferBytesAllocated.addAndGet(-buffer_.array().length);

        if (response_.len() == 0) {
            // go straight to reading again. this was probably an oneway method
            state_ = FrameBufferState.AWAITING_REGISTER_READ;
            buffer_ = null;
        } else {
            buffer_ = ByteBuffer.wrap(response_.get(), 0, response_.len());

            // set state that we're waiting to be switched to write. we do this
            // asynchronously through requestSelectInterestChange() because there is
            // a possibility that we're not in the main thread, and thus currently
            // blocked in select(). (this functionality is in place for the sake of
            // the HsHa server.)
            state_ = FrameBufferState.AWAITING_REGISTER_WRITE;
        }
        requestSelectInterestChange();
    }

    /**
     * Actually invoke the method signified by this FrameBuffer.
     */
    public void invoke() {
        frameTrans_.reset(buffer_.array());
        response_.reset();

        try {
            if (eventHandler_ != null) {
                eventHandler_.processContext(context_, inTrans_, outTrans_);
            }
            processorFactory_.getProcessor(inTrans_).process(inProt_, outProt_);
            responseReady();
            return;
        } catch (TException te) {
            LOGGER.warn("Exception while invoking!", te);
        } catch (Throwable t) {
            LOGGER.error("Unexpected throwable while invoking!", t);
        }
        // This will only be reached when there is a throwable.
        state_ = FrameBufferState.AWAITING_CLOSE;
        requestSelectInterestChange();
    }

    /**
     * Perform a read into buffer.
     *
     * @return true if the read succeeded, false if there was an error or the
     * connection closed.
     */
    private boolean internalRead() {
        try {
            if (trans_.read(buffer_) < 0) {
                return false;
            }
            return true;
        } catch (IOException e) {
            LOGGER.warn("Got an IOException in internalRead!", e);
            return false;
        }
    }

    /**
     * We're done writing, so reset our interest ops and change state
     * accordingly.
     */
    private void prepareRead() {
        // we can set our interest directly without using the queue because
        // we're in the select thread.
        selectionKey_.interestOps(SelectionKey.OP_READ);
        // get ready for another go-around
        buffer_ = ByteBuffer.allocate(4);
        state_ = FrameBufferState.READING_FRAME_SIZE;
    }

    /**
     * When this FrameBuffer needs to change its select interests and execution
     * might not be in its select thread, then this method will make sure the
     * interest change gets done when the select thread wakes back up. When the
     * current thread is this FrameBuffer's select thread, then it just does the
     * interest change immediately.
     */
    protected void requestSelectInterestChange() {
        if (Thread.currentThread() == this.selectThread_) {
            changeSelectInterests();
        } else {
            this.selectThread_.requestSelectInterestChange(this);
        }
    }
} // FrameBuffer