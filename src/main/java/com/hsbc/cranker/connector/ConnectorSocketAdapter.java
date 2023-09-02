package com.hsbc.cranker.connector;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;

import static com.hsbc.cranker.connector.CrankerConnectorBuilder.CRANKER_PROTOCOL_1;
import static com.hsbc.cranker.connector.CrankerConnectorBuilder.CRANKER_PROTOCOL_3;


/**
 * Adaptor for different protocol implementation for cranker v3 and v1
 */
public class ConnectorSocketAdapter implements WebSocket.Listener, ConnectorSocket {

    private final URI targetURI;
    private final HttpClient httpClient;
    private final ConnectorSocketListener listener;
    private final ProxyEventListener proxyEventListener;
    private final ScheduledExecutorService executor;

    private WebSocket.Listener underlying;
    private ConnectorSocket underlying2;

    private String protocol = "N/A";

    ConnectorSocketAdapter(URI targetURI, HttpClient httpClient, ConnectorSocketListener listener,
                                  ProxyEventListener proxyEventListener, ScheduledExecutorService executor) {
        this.targetURI = targetURI;
        this.httpClient = httpClient;
        this.proxyEventListener = proxyEventListener;
        this.executor = executor;

        // bridge the listener, listener observer will only see the adapter consistently.
        // e.g. RouterRegistration.idleSockets and RouterRegistration.runningSockets only see ConnectorSocketAdapter instances
        final ConnectorSocketAdapter adapter = this;
        this.listener = new ConnectorSocketListener() {
            @Override
            public void onConnectionAcquired(ConnectorSocket socket) {
                listener.onConnectionAcquired(adapter);
            }

            @Override
            public void onClose(ConnectorSocket socket, Throwable error) {
                listener.onClose(adapter, error);
            }
        };
    }

    @Override
    public State state() {
        return underlying2 != null ? underlying2.state() : State.NOT_STARTED;
    }

    @Override
    public String version() {
        return underlying2 != null ? underlying2.version() : "";
    }

    void close() {
        if (CRANKER_PROTOCOL_3.equals(protocol) && underlying2 != null) {
            ((ConnectorSocketV3) underlying2).close();
        } else if (CRANKER_PROTOCOL_1.equals(protocol) && underlying2 != null) {
            ((ConnectorSocketImpl) underlying2).close();
        }
    }

    void updateState(State state) {
        if (CRANKER_PROTOCOL_3.equals(protocol) && underlying2 != null) {
            ((ConnectorSocketV3) underlying2).updateState(state);
        } else if (CRANKER_PROTOCOL_1.equals(protocol) && underlying2 != null) {
            ((ConnectorSocketImpl) underlying2).updateState(state);
        }
    }

    @Override
    public void onOpen(WebSocket webSocket) {
        final String subProtocol = webSocket.getSubprotocol();
        if (CRANKER_PROTOCOL_3.equals(subProtocol)) {
            final ConnectorSocketV3 connectorSocketV3 = new ConnectorSocketV3(targetURI, httpClient, listener, proxyEventListener, executor);
            protocol = CRANKER_PROTOCOL_3;
            underlying = connectorSocketV3;
            underlying2 = connectorSocketV3;
        } else {
            final ConnectorSocketImpl connectorSocket = new ConnectorSocketImpl(targetURI, httpClient, listener, proxyEventListener, executor);
            protocol = CRANKER_PROTOCOL_1;
            underlying = connectorSocket;
            underlying2 = connectorSocket;
        }
        underlying.onOpen(webSocket);
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
        return underlying.onText(webSocket, data, last);
    }

    @Override
    public CompletionStage<?> onBinary(WebSocket webSocket, ByteBuffer data, boolean last) {
        return underlying.onBinary(webSocket, data, last);
    }

    @Override
    public CompletionStage<?> onPing(WebSocket webSocket, ByteBuffer message) {
        return underlying.onPing(webSocket, message);
    }

    @Override
    public CompletionStage<?> onPong(WebSocket webSocket, ByteBuffer message) {
        return underlying.onPong(webSocket, message);
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
        return underlying.onClose(webSocket, statusCode, reason);
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
        underlying.onError(webSocket, error);
    }

    /**
     * Adapt the complete action to cranker v3/v1 implementation.
     * @return CompletableFuture which resolved when the complete action done
     */
    public CompletableFuture<Void> complete() {
        if (CRANKER_PROTOCOL_3.equals(protocol) && underlying != null) {
            return ((ConnectorSocketV3) underlying).complete();
        } else if (CRANKER_PROTOCOL_1.equals(protocol) && underlying != null) {
            return ((ConnectorSocketImpl) underlying).complete();
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * get the underlying cranker protocol which the socket using.
     * @return protocol
     */
    public String getProtocol() {
        return protocol;
    }
}
