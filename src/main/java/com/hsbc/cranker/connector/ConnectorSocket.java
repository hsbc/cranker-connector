package com.hsbc.cranker.connector;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * A single connection between a connector and a router
 */
public interface ConnectorSocket {
    /**
     * The state of the connection from this connector to a router
     */
    enum State {
        /**
         * No connection attempt has been made yet
         */
        NOT_STARTED(false),
        /**
         * The socket is connected to the router and is ready to receive a request
         */
        IDLE(false),
        /**
         * The socket is currently handling a request
         */
        HANDLING_REQUEST(false),
        /**
         * A request was successfully completed on this socket
         */
        COMPLETE(true),
        /**
         * An error occurred on this socket
         */
        ERROR(true),
        /**
         * The router that this was connected to was shut down
         */
        ROUTER_CLOSED(true);

        final private boolean isCompleted;

        State(boolean isCompleted) {
            this.isCompleted = isCompleted;
        }

        /**
         * @return true if it's in end state
         */
        public boolean isCompleted() {
            return isCompleted;
        }
    }

    /**
     * @return The current state of this connection
     */
    State state();
}

class ConnectorSocketImpl implements WebSocket.Listener, ConnectorSocket {

    private volatile ScheduledFuture<?> timeoutTask;
    private volatile Flow.Subscriber<? super ByteBuffer> targetBodySubscriber;

    private static final byte[] PING_MSG = "ping".getBytes(StandardCharsets.UTF_8);
    private HttpRequest requestToTarget;
    private final URI targetURI;
    private final HttpClient httpClient;
    private final ConnectorSocketListener listener;
    private WebSocket webSocket;
    private final ScheduledExecutorService executor;
    private ScheduledFuture<?> pingPongTask;
    private volatile State state = State.NOT_STARTED;
    private volatile CompletableFuture<Void> complete = new CompletableFuture<>();

    ConnectorSocketImpl(URI targetURI, HttpClient httpClient, ConnectorSocketListener listener, ScheduledExecutorService executor) {
        this.targetURI = targetURI;
        this.httpClient = httpClient;
        this.listener = listener;
        this.executor = executor;
        onSignOfLife();
    }

    private void onTimeout() {
        onError(webSocket, new TimeoutException("No message received from router socket"));
    }

    private void newRequestToTarget(CrankerRequestParser protocolRequest, WebSocket webSocket) {
        CrankerResponseBuilder protocolResponse = CrankerResponseBuilder.newBuilder();

        URI dest = targetURI.resolve(protocolRequest.dest);

        HttpRequest.BodyPublisher bodyPublisher;
        if (protocolRequest.requestBodyPending()) {
            bodyPublisher = new TargetRequestBodyPublisher(protocolRequest, webSocket);
        } else {
            bodyPublisher = HttpRequest.BodyPublishers.noBody();
            webSocket.request(1);
        }

        HttpRequest.Builder rb = HttpRequest.newBuilder()
            .uri(dest)
            .method(protocolRequest.httpMethod, bodyPublisher);
        putHeadersTo(rb, protocolRequest);

        this.requestToTarget = rb.build();
        HttpResponse.BodyHandler<Void> bh = new TargetResponseHandler(protocolResponse, webSocket);

        httpClient.sendAsync(requestToTarget, bh);
    }

    private void putHeadersTo(HttpRequest.Builder requestToTarget, CrankerRequestParser crankerRequestParser) {
        for (String line : crankerRequestParser.headers) {
            int pos = line.indexOf(':');
            // this will ignore HTTP/2 pseudo request headers like :method, :path, :authority
            if (pos > 0) {
                String header = line.substring(0, pos).trim().toLowerCase();
                String value = line.substring(pos + 1);
                if (!HttpUtils.DISALLOWED_REQUEST_HEADERS.contains(header)) {
                    requestToTarget.header(header, value);
                }
            }
        }
    }

    @Override
    public void onOpen(WebSocket webSocket) {
        this.webSocket = webSocket;
        onSignOfLife();
        updateState(State.IDLE);
        webSocket.request(1);
        pingPongTask = executor.scheduleAtFixedRate(() -> {
            try {
                ByteBuffer wrap = ByteBuffer.wrap(PING_MSG);
                webSocket.sendPing(wrap);
            } catch (Exception e) {
                close(State.ERROR, 1011, e);
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    private void onSignOfLife() {
        cancelTimeout();
        timeoutTask = executor.schedule(this::onTimeout, 20, TimeUnit.SECONDS);
    }

    private void cancelTimeout() {
        if (timeoutTask != null) {
            timeoutTask.cancel(false);
            timeoutTask = null;
        }
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
        onSignOfLife();
        if (requestToTarget == null) {
            CrankerRequestParser protocolRequest = new CrankerRequestParser(data);
            listener.onConnectionAcquired(this);
            newRequestToTarget(protocolRequest, webSocket);
            updateState(State.HANDLING_REQUEST);
        } else if (CrankerRequestParser.REQUEST_BODY_ENDED_MARKER.contentEquals(data)) {
            targetBodySubscriber.onComplete();
            webSocket.request(1);
        }
        return null;
    }

    @Override
    public CompletionStage<?> onBinary(WebSocket webSocket, ByteBuffer data, boolean last) {
        onSignOfLife();
        // returning null from here tells java that it can reuse the data buffer right away, so need a copy
        int capacity = data.remaining();
        ByteBuffer copy = data.isDirect() ? ByteBuffer.allocateDirect(capacity) : ByteBuffer.allocate(capacity);
        copy.put(data);
        copy.rewind();
        targetBodySubscriber.onNext(copy);
        webSocket.request(1);
        return null;
    }

    @Override
    public CompletionStage<?> onPing(WebSocket webSocket, ByteBuffer message) {
        onSignOfLife();
        webSocket.request(1);
        return null;
    }

    @Override
    public CompletionStage<?> onPong(WebSocket webSocket, ByteBuffer message) {
        onSignOfLife();
        webSocket.request(1);
        return null;
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
        close(State.ROUTER_CLOSED, WebSocket.NORMAL_CLOSURE, null);
        return null;
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
        close(State.ERROR, 1011, error);
    }

    public void close(State newState, int statusCode, Throwable error) {
        updateState(newState);
        cancelTimeout();
        if (pingPongTask != null) {
            pingPongTask.cancel(false);
            pingPongTask = null;
        }
        if (!webSocket.isOutputClosed()) {
            webSocket.sendClose(statusCode, "");
        }
        listener.onClose(this, error);
    }

    @Override
    public State state() {
        return state;
    }

    protected CompletableFuture<Void> complete() {
        return complete;
    }

    public void updateState(State state) {
        this.state = state;
        if (state.isCompleted()) {
            complete.complete(null);
        }
    }

    @Override
    public String toString() {
        return "ConnectorSocket{" +
            "targetURI=" + targetURI +
            ", state=" + state +
            ", request=" + requestToTarget +
            '}';
    }

    private class TargetResponseHandler implements HttpResponse.BodyHandler<Void> {
        private final CrankerResponseBuilder protocolResponse;
        private final WebSocket webSocket;

        public TargetResponseHandler(CrankerResponseBuilder protocolResponse, WebSocket webSocket) {
            this.protocolResponse = protocolResponse;
            this.webSocket = webSocket;
        }

        @Override
        public HttpResponse.BodySubscriber<Void> apply(HttpResponse.ResponseInfo responseInfo) {

            protocolResponse
                .withResponseStatus(responseInfo.statusCode())
                .withResponseReason("TODO");

            for (Map.Entry<String, List<String>> header : responseInfo.headers().map().entrySet()) {
                for (String value : header.getValue()) {
                    protocolResponse.withHeader(header.getKey(), value);
                }
            }

            String respHeaders = protocolResponse.build();
            CompletableFuture<WebSocket> headersSentFuture = webSocket.sendText(respHeaders, true)
                .whenComplete((webSocket1, throwable) -> {
                    if (throwable != null) {
                        close(State.ERROR, 1011, throwable);
                    }
                });

            return HttpResponse.BodySubscribers.fromSubscriber(new Flow.Subscriber<>() {

                private Flow.Subscription subscription;
                private volatile CompletableFuture<WebSocket> bodySendingFuture;

                @Override
                public void onSubscribe(Flow.Subscription subscription) {
                    this.subscription = subscription;
                    headersSentFuture.whenComplete((ws, throwable) -> {
                        if (throwable != null) {
                            subscription.cancel();
                        } else {
                            subscription.request(1);
                        }
                    });
                }

                @Override
                public void onNext(List<ByteBuffer> items) {

                    if (items.isEmpty()) {
                        subscription.request(1);
                        return;
                    }

                    if (webSocket.isOutputClosed() || state != State.HANDLING_REQUEST) {
                        subscription.cancel();
                        onError(new RuntimeException("Error sending response body, output channel is closed"));
                        return;
                    }

                    CompletableFuture<WebSocket> completableFuture = webSocket.sendBinary(items.get(0), true);
                    for (ByteBuffer item : items.subList(1, items.size())) {
                        completableFuture = completableFuture.thenCompose(websocket -> websocket.sendBinary(item, true));
                    }

                    completableFuture.whenComplete((ws, error) -> {
                        if (error != null) {
                            subscription.cancel();
                            onError(error);
                        } else {
                            subscription.request(1);
                        }
                    });

                    bodySendingFuture = completableFuture;
                }

                @Override
                public void onError(Throwable throwable) {
                    close(State.ERROR, 1011, throwable);
                }

                @Override
                public void onComplete() {
                    if (bodySendingFuture != null) {
                        bodySendingFuture.whenComplete((ws, error) -> {
                            if (error != null) {
                                onError(error);
                            } else {
                                close(State.COMPLETE, WebSocket.NORMAL_CLOSURE, null);
                            }
                        });
                    } else {
                        close(State.COMPLETE, WebSocket.NORMAL_CLOSURE, null);
                    }
                }
            });
        }
    }

    private class TargetRequestBodyPublisher implements HttpRequest.BodyPublisher {
        private final CrankerRequestParser protocolRequest;
        private final WebSocket webSocket;

        public TargetRequestBodyPublisher(CrankerRequestParser protocolRequest, WebSocket webSocket) {
            this.protocolRequest = protocolRequest;
            this.webSocket = webSocket;
        }

        @Override
        public long contentLength() {
            return protocolRequest.bodyLength();
        }

        @Override
        public void subscribe(Flow.Subscriber<? super ByteBuffer> subscriber) {
            targetBodySubscriber = subscriber;
            subscriber.onSubscribe(new Flow.Subscription() {
                @Override
                public void request(long n) {
                    webSocket.request(n);
                }

                @Override
                public void cancel() {
                    close(State.ERROR, 1011, null);
                }
            });
        }
    }
}
