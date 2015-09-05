package me.chunyu.rpc_proxy.server;

/**
 * Possible states for the FrameBuffer state machine.
 */
public enum FrameBufferState {

    // in the midst of reading the frame size off the wire
    READING_FRAME_SIZE,
    // reading the actual frame data now, but not all the way done yet
    READING_FRAME,
    // completely read the frame, so an invocation can now happen
    READ_FRAME_COMPLETE,
    // waiting to get switched to listening for write events
    AWAITING_REGISTER_WRITE,
    // started writing response data, not fully complete yet
    WRITING,
    // another thread wants this framebuffer to go back to reading
    AWAITING_REGISTER_READ,
    // we want our transport and selection key invalidated in the selector
    // thread
    AWAITING_CLOSE
}