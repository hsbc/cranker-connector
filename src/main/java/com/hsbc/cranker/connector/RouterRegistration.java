package com.hsbc.cranker.connector;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Information about connections to a router.
 * <p>To get this data, call {@link CrankerConnector#routers()}</p>
 */
public interface RouterRegistration {

    /**
     * @return The number of expected idle connections to this router
     */
    int expectedWindowSize();

    /**
     * @return The current number of idle connections
     */
    int idleSocketSize();

    /**
     * @return The sockets that are currently connected to the router, ready to process a request
     */
    Collection<ConnectorSocket> idleSockets();

    /**
     * @return The router's websocket registration URI
     */
    URI registrationUri();

    /**
     * @return The current state of the connection to this router
     */
    State state();

    /**
     * If a router is unavailable, then the connector will repeatedly retry (with exponential backoff, up to 10 seconds).
     * This returns the current number of failed attempts. When an attempt is succesful, this is reset to 0.
     *
     * @return The current number of failed attempts to connect to this router
     */
    int currentUnsuccessfulConnectionAttempts();

    /**
     * @return If this socket is not connected due to an error, then this is the reason; otherwise this is null.
     */
    Throwable lastConnectionError();

    /**
     * The state of a router from the connector's point of view
     */
    enum State {
        /**
         * The connection to the router has not been established yet
         */
        NOT_STARTED,
        /**
         * The connection between this connector and router is active
         */
        ACTIVE,
        /**
         * Graceful shutdown is in progress
         */
        STOPPING,
        /**
         * The connection to the router has been closed
         */
        STOPPED
    }

}

class RouterRegistrationImpl implements ConnectorSocketListener, RouterRegistration {

    private final static String CRANKER_PROTOCOL = "CrankerProtocol"; // 1.0

    private volatile State state = State.NOT_STARTED;
    private final List<String> preferredProtocols;
    private final HttpClient client;
    private final URI registrationUri;
    private final String domain;
    private final String route;
    private final int windowSize;
    private final Set<ConnectorSocket> idleSockets = ConcurrentHashMap.newKeySet();
    private final Set<ConnectorSocket> runningSockets = ConcurrentHashMap.newKeySet();
    private final URI targetUri;
    private final ScheduledExecutorService executor;
    private final AtomicInteger connectAttempts = new AtomicInteger();
    private volatile Throwable lastConnectionError;
    private final RouterEventListener routerEventListener;
    private final ProxyEventListener proxyEventListener;
    private final AtomicBoolean isAddMissingScheduled = new AtomicBoolean(false);

    RouterRegistrationImpl(List<String> preferredProtocols, HttpClient client, URI registrationUri, String domain, String route, int windowSize, URI targetUri,
                           ScheduledExecutorService executor, RouterEventListener routerEventListener,
                           ProxyEventListener proxyEventListener) {
        this.preferredProtocols = preferredProtocols;
        this.client = client;
        this.registrationUri = registrationUri;
        this.domain = domain;
        this.route = route;
        this.windowSize = windowSize;
        this.targetUri = targetUri;
        this.executor = executor;
        this.routerEventListener = routerEventListener;
        this.proxyEventListener = proxyEventListener;
    }

    void start() {
        state = State.ACTIVE;
        addAnyMissing();
    }

    CompletableFuture<Void> stop() {
        state = State.STOPPING;
        URI deregisterUri = registrationUri.resolve("/deregister/?" + registrationUri.getRawQuery());
        return client.newWebSocketBuilder()
            .header(CRANKER_PROTOCOL, "1.0") // for backward compatibility
            .header("Route", this.route)
            .header("Domain", this.domain)
            .buildAsync(deregisterUri, new WebSocket.Listener() {
                @Override
                public void onOpen(WebSocket webSocket) {
                    webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "");
                }
            })
            .thenCompose(webSocket -> {
                final Stream<ConnectorSocket> runningSocketStream = this.runningSockets.stream();
                final Stream<ConnectorSocket> idleSocketStream = this.idleSockets.stream().peek(socket -> {
                    ConnectorSocketAdapter adapter = (ConnectorSocketAdapter) socket;
                    if (!adapter.state().isCompleted()) {
                        adapter.updateState(ConnectorSocket.State.STOPPING);
                    }
                });
                return CompletableFuture.allOf(Stream
                    .concat(runningSocketStream, idleSocketStream)
                    .map(socket -> ((ConnectorSocketAdapter) socket).complete())
                    .collect(Collectors.toList())
                    .toArray(CompletableFuture[]::new)
                );
            })
            .handle((webSocket, throwable) -> {
                state = State.STOPPED;
                return null;
            });
    }

    private static String[] getLessPreferredProtocol(List<String> protocols) {
        return protocols.size() > 1 ? protocols.subList(1, protocols.size()).toArray(new String[0]) : new String[0];
    }

    private void addAnyMissing() {
        while (state == State.ACTIVE && idleSockets.size() < windowSize) {

            ConnectorSocketAdapter connectorSocket = new ConnectorSocketAdapter(
                targetUri, client, this, proxyEventListener, executor
            );
            idleSockets.add(connectorSocket);

            client.newWebSocketBuilder()
                .header(CRANKER_PROTOCOL, "1.0") // for backward compatibility
                .subprotocols(preferredProtocols.get(0), getLessPreferredProtocol(preferredProtocols))
                .header("Route", route)
                .header("Domain", domain)
                .connectTimeout(Duration.ofMillis(5000))
                .buildAsync(registrationUri, connectorSocket)
                .whenComplete((webSocket, throwable) -> {
                    if (throwable == null) {
                        connectAttempts.set(0);
                        lastConnectionError = null;
                    } else {
                        lastConnectionError = throwable;
                        idleSockets.remove(connectorSocket);
                        if (routerEventListener != null) {
                            routerEventListener.onSocketConnectionError(this, throwable);
                        }

                        if (isAddMissingScheduled.compareAndSet(false, true)) {
                            connectAttempts.incrementAndGet();
                            executor.schedule(() -> {
                                isAddMissingScheduled.set(false);
                                addAnyMissing();
                            }, retryAfterMillis(), TimeUnit.MILLISECONDS);
                        }
                    }
                });
        }
    }

    /**
     * @return Milliseconds to wait until trying again, increasing exponentially, capped at 10 seconds
     */
    private int retryAfterMillis() {
        return 500 + Math.min(10000, (int) Math.pow(2, connectAttempts.get()));
    }

    @Override
    public void onConnectionAcquired(ConnectorSocket socket) {
        runningSockets.add(socket);
        idleSockets.remove(socket);
        addAnyMissing();
    }

    @Override
    public void onClose(ConnectorSocket socket, Throwable error) {
        runningSockets.remove(socket);
        idleSockets.remove(socket);
        addAnyMissing();
    }

    @Override
    public int expectedWindowSize() {
        return windowSize;
    }

    @Override
    public int idleSocketSize() {
        return idleSockets.size();
    }

    @Override
    public Collection<ConnectorSocket> idleSockets() {
        return idleSockets;
    }

    @Override
    public URI registrationUri() {
        return registrationUri;
    }

    @Override
    public State state() {
        return state;
    }

    @Override
    public int currentUnsuccessfulConnectionAttempts() {
        return connectAttempts.get();
    }

    @Override
    public Throwable lastConnectionError() {
        return lastConnectionError;
    }

    @Override
    public String toString() {
        return "RouterRegistration{" +
            "state=" + state +
            ", registrationUri=" + registrationUri +
            ", route='" + route + '\'' +
            ", windowSize=" + windowSize +
            ", targetUri=" + targetUri +
            ", connectAttempts=" + connectAttempts +
            ", lastConnectionError=" + lastConnectionError +
            ", idleSockets=" + idleSockets +
            '}';
    }

    static class Factory {
        private final List<String> preferredProtocols;
        private final HttpClient client;
        private final String domain;
        private final String route;
        private final int windowSize;
        private final URI targetUri;
        private volatile ScheduledExecutorService executor;
        private final RouterEventListener routerEventListener;
        private final ProxyEventListener proxyEventListener;

        Factory(List<String> preferredProtocols, HttpClient client, String domain, String route, int windowSize, URI targetUri,
                RouterEventListener routerEventListener, ProxyEventListener proxyEventListener) {
            this.preferredProtocols = preferredProtocols;
            this.client = client;
            this.domain = domain;
            this.route = route;
            this.windowSize = windowSize;
            this.targetUri = targetUri;
            this.routerEventListener = routerEventListener;
            this.proxyEventListener = proxyEventListener;
        }

        RouterRegistrationImpl create(URI registrationUri) {
            return new RouterRegistrationImpl(preferredProtocols, client, registrationUri, domain, route, windowSize, targetUri, executor, routerEventListener, proxyEventListener);
        }

        void start() {
            executor = Executors.newSingleThreadScheduledExecutor();
        }

        void stop() {
            executor.shutdownNow();
        }
    }

}
