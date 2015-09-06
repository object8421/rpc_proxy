package me.chunyu.rpc_proxy.os;

/**
 * Listener interface which receives signals from {@link ExitHandler} when about
 * to exit app.
 *
 * @author Liviu Tudor http://about.me/liviutudor
 */
public interface ExitSignalListener {
    /**
     * Only notification sent when the program is about to exit. Listeners can
     * close resources, cleanup etc.
     */
    void notifyExit();
}
