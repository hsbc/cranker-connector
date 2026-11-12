package com.hsbc.cranker.connector;

import java.net.http.HttpRequest;
import java.util.List;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

final class ConnectorInstrumentation {
    private static final Logger LOG = Logger.getLogger(ConnectorInstrumentation.class.getName());
    private static final Runnable[] NO_CLEANUP = new Runnable[0];
    private final List<ConnectorInstrumentationListener> listeners;

    ConnectorInstrumentation(List<ConnectorInstrumentationListener> listeners) {
        this.listeners = List.copyOf(listeners);
    }

    Runnable[] open(HttpRequest request) {
        if (listeners.isEmpty()) {
            return NO_CLEANUP;
        }
        Runnable[] cleanup = new Runnable[listeners.size()];
        TargetDispatchInfo info = new TargetDispatchInfo(request);
        for (int i = 0; i < listeners.size(); i++) {
            try {
                cleanup[i] = Objects.requireNonNull(listeners.get(i).beforeTargetDispatch(info),
                    "beforeTargetDispatch must return a cleanup Runnable");
            } catch (VirtualMachineError fatal) {
                try {
                    close(cleanup);
                } catch (Throwable cleanupFailure) {
                    if (cleanupFailure != fatal) fatal.addSuppressed(cleanupFailure);
                }
                throw fatal;
            } catch (Throwable failure) {
                LOG.log(Level.WARNING, "Connector instrumentation callback failed; request forwarding continues", failure);
            }
        }
        return cleanup;
    }

    static void close(Runnable[] cleanup) {
        for (int i = cleanup.length - 1; i >= 0; i--) {
            if (cleanup[i] == null) {
                continue;
            }
            try {
                cleanup[i].run();
            } catch (VirtualMachineError fatal) {
                throw fatal;
            } catch (Throwable cleanupFailure) {
                LOG.log(Level.WARNING, "Connector instrumentation cleanup failed; request forwarding continues", cleanupFailure);
            }
        }
    }
}
