package me.chunyu.rpc_proxy.server;

import java.nio.ByteBuffer;

public interface RequestHandler {
    public boolean requestInvoke(FrameBuffer frameBuffer);
}
