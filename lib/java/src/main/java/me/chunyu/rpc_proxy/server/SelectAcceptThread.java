package me.chunyu.rpc_proxy.server;

import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransportException;
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

    protected final Selector selector;

    // List of FrameBuffers that want to change their selection interests.
    protected final Set<FrameBuffer> selectInterestChanges = new HashSet<FrameBuffer>();

    protected final RequestHandler handler;
    private final TNonblockingServerTransport serverTransport;
    private AtomicBoolean stopped;

    public SelectAcceptThread(final TNonblockingServerTransport serverTransport, RequestHandler handler, AtomicBoolean stopped) throws IOException {
        this.handler = handler;
        this.serverTransport = serverTransport;
        this.selector = SelectorProvider.provider().openSelector();
        this.serverTransport.registerSelector(this.selector);
        this.stopped = stopped;
    }

    public boolean isStopped() {
        return stopped.get();
    }

    /**
     * If the selector is blocked, wake it up.
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

    /**
     * Do the work required to read from a readable client. If the frame is
     * fully read, then invoke the method call.
     */
    protected void handleRead(SelectionKey key) {
        FrameBuffer buffer = (FrameBuffer) key.attachment();
        if (!buffer.read()) {
            cleanupSelectionKey(key);
            return;
        }

        // if the buffer's frame read is complete, invoke the method.
        if (buffer.isFrameFullyRead()) {
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
     * Do connection-close cleanup on a given SelectionKey.
     */
    protected void cleanupSelectionKey(SelectionKey key) {
        // remove the records from the two maps
        FrameBuffer buffer = (FrameBuffer) key.attachment();
        if (buffer != null) {
            // close the buffer
            buffer.close();
        }
        // cancel the selection key
        key.cancel();
    }


    /**
     * The work loop. Handles both selecting (all IO operations) and managing
     * the selection preferences of all existing connections.
     */
    public void run() {
        try {

            while (!stopped.get()) {
                select();
                processInterestChanges();
            }
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
     * If there are connections to be accepted, accept them.
     * If there are existing connections with data waiting to be read, read it,
     * buffering until a whole frame has been read.
     * If there are any pending responses, buffer them until their target client
     * is available, and then send the data.
     */
    private void select() {
        try {
            // wait for io events.
            selector.select();

            // process the io events we received
            Iterator<SelectionKey> selectedKeys = selector.selectedKeys().iterator();
            while (!stopped.get() && selectedKeys.hasNext()) {
                SelectionKey key = selectedKeys.next();
                selectedKeys.remove();

                // skip if not valid
                if (!key.isValid()) {
                    cleanupSelectionKey(key);
                    continue;
                }

                // if the key is marked Accept, then it has to be the server
                // transport.
                if (key.isAcceptable()) {
                    handleAccept();
                } else if (key.isReadable()) {
                    // deal with reads
                    handleRead(key);
                } else if (key.isWritable()) {
                    // deal with writes
                    handleWrite(key);
                } else {
                    LOGGER.warn("Unexpected state in select! " + key.interestOps());
                }
            }
        } catch (IOException e) {
            LOGGER.warn("Got an IOException while selecting!", e);
        }
    }

    protected FrameBuffer createFrameBuffer(final TNonblockingTransport trans,
                                            final SelectionKey selectionKey,
                                            final SelectAcceptThread selectThread) {
        return null;
//        return processorFactory_.isAsyncProcessor() ?
//                new AsyncFrameBuffer(trans, selectionKey, selectThread) :
//                new FrameBuffer(trans, selectionKey, selectThread);
    }

    /**
     * Accept a new connection.
     */
    private void handleAccept() throws IOException {
        SelectionKey clientKey = null;
        TNonblockingTransport client = null;
        try {
            // accept the connection
            client = (TNonblockingTransport) serverTransport.accept();
            clientKey = client.registerSelector(selector, SelectionKey.OP_READ);

            // add this key to the map
            FrameBuffer frameBuffer = createFrameBuffer(client, clientKey, SelectAcceptThread.this);

            clientKey.attach(frameBuffer);
        } catch (TTransportException tte) {
            // something went wrong accepting.
            LOGGER.warn("Exception trying to accept!", tte);
            tte.printStackTrace();
            if (clientKey != null) cleanupSelectionKey(clientKey);
            if (client != null) client.close();
        }
    }
} // SelectAcceptThread