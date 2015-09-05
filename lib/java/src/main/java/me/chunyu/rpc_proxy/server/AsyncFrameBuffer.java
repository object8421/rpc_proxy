package me.chunyu.rpc_proxy.server;

import org.apache.thrift.TBaseAsyncProcessor;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TNonblockingTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.SelectionKey;

public class AsyncFrameBuffer extends FrameBuffer {
    protected final Logger LOGGER = LoggerFactory.getLogger(getClass().getName());

    public AsyncFrameBuffer(TNonblockingTransport trans, SelectionKey selectionKey, SelectAcceptThread selectThread) {
        super(trans, selectionKey, selectThread);
    }

    public TProtocol getInputProtocol() {
        return inProt_;
    }

    public TProtocol getOutputProtocol() {
        return outProt_;
    }


    public void invoke() {
        frameTrans_.reset(buffer_.array());
        response_.reset();

        try {
            // TODO:
            throw new TException("");
//            if (eventHandler_ != null) {
//                eventHandler_.processContext(context_, inTrans_, outTrans_);
//            }
//            ((TBaseAsyncProcessor) processorFactory_.getProcessor(inTrans_)).process(this);
//            return;
        } catch (TException te) {
            LOGGER.warn("Exception while invoking!", te);
        } catch (Throwable t) {
            LOGGER.error("Unexpected throwable while invoking!", t);
        }
        // This will only be reached when there is a throwable.
        state_ = FrameBufferState.AWAITING_CLOSE;
        requestSelectInterestChange();
    }
}