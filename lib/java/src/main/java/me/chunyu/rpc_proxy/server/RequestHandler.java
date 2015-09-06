package me.chunyu.rpc_proxy.server;

public interface RequestHandler {
    public boolean requestInvoke(FrameBuffer frameBuffer);
}
