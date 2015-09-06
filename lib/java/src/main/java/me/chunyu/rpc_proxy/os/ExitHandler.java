package me.chunyu.rpc_proxy.os;

import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.util.HashSet;
import java.util.Set;

/**
 * Simple handler which exists the app cleanly.
 *
 * @author Liviu Tudor http://about.me/liviutudor
 */
public class ExitHandler implements SignalHandler {
    /**
     * Listeners to notify when signal is received.
     */
    private Set<ExitSignalListener> listeners = new HashSet<ExitSignalListener>();

    public void addListener(ExitSignalListener l) {
        listeners.add(l);
    }

    public void removeListener(ExitSignalListener l) {
        listeners.remove(l);
    }

    /**
     * Handle the signal: simply pass the message to the registered listeners.
     * After all notification is done simply exit.
     */
    public void handle(Signal sig) {
        for (ExitSignalListener l : listeners) {
            l.notifyExit();
        }
        System.exit(0);
    }
}
