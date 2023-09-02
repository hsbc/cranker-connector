package com.hsbc.cranker.connector;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * A single connection between a connector and a router in protocol cranker_v3 implementation
 */
public class ConnectorSocketV3 implements WebSocket.Listener, ConnectorSocket {

    static final byte MESSAGE_TYPE_DATA = 0;
    static final byte MESSAGE_TYPE_HEADER = 1;
    static final byte MESSAGE_TYPE_RST_STREAM = 3;
    static final byte MESSAGE_TYPE_WINDOW_UPDATE = 8;

    private volatile ScheduledFuture<?> timeoutTask;

    private volatile Map<Integer, RequestContext> contextMap = new ConcurrentHashMap<>();

    private static final byte[] PING_MSG = "ping".getBytes(StandardCharsets.UTF_8);
    private final URI targetURI;
    private final HttpClient httpClient;
    private final ConnectorSocketListener listener;
    private final ProxyEventListener proxyEventListener;
    private WebSocket webSocket;
    private volatile State websocketState;
    private final ScheduledExecutorService executor;
    private ScheduledFuture<?> pingPongTask;
    private final List<BufferHolder> unCompletedBuffers;

    private final CompletableFuture<Void> complete = new CompletableFuture<>();
    private final ConcurrentLinkedQueue<BinarySendingTask> sendingTasks = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean isSending = new AtomicBoolean(false);

    ConnectorSocketV3(URI targetURI, HttpClient httpClient, ConnectorSocketListener listener,
                      ProxyEventListener proxyEventListener, ScheduledExecutorService executor) {
        this.targetURI = targetURI;
        this.httpClient = httpClient;
        this.listener = listener;
        this.proxyEventListener = proxyEventListener;
        this.executor = executor;
        this.unCompletedBuffers = new ArrayList<>();
        onSignOfLife();
    }

    private void onTimeout() {
        onError(webSocket, new TimeoutException("No message received from router socket"));
    }

    private void newRequestToTarget(RequestContext context, CrankerRequest protocolRequest, WebSocket webSocket, boolean isStreamEnd) {

        URI dest = targetURI.resolve(protocolRequest.dest);

        HttpRequest.BodyPublisher bodyPublisher;
        if (!isStreamEnd) {
            bodyPublisher = new TargetRequestBodyPublisher(context, protocolRequest, webSocket);
        } else {
            bodyPublisher = HttpRequest.BodyPublishers.noBody();
        }

        HttpRequest.Builder rb = HttpRequest.newBuilder()
            .uri(dest)
            .method(protocolRequest.httpMethod, bodyPublisher);

        putHeadersTo(rb, protocolRequest);

        final HttpRequest requestToTarget = proxyEventListener.beforeProxyToTarget(rb.build(), rb);
        context.request = requestToTarget;
        HttpResponse.BodyHandler<Void> bh = new TargetResponseHandlerV3(context,
            CrankerResponseBuilder.newBuilder(),
            webSocket);

        final CompletableFuture<HttpResponse<Void>> responseFuture = httpClient.sendAsync(requestToTarget, bh);
        context.responseFuture = responseFuture;
        responseFuture.whenComplete((response, throwable) -> {
            if (throwable != null) {
                resetStream(context.requestId, 1011, "target request failed: " + throwable.getMessage());
            }
        });
    }

    /**
     * Calling websocket.sendBinary() will result in exceptions when previous sending action not completed.
     * This simple wrapper is for making sure the sendBinary() action is in queue and complete one by one.
     */
    private CompletableFuture<WebSocket> sendBinary(ByteBuffer data, boolean last) {
        return sendInQueue(new BinarySendingTask(data, last));
    }

    private class BinarySendingTask {
        final ByteBuffer data;
        final boolean last;
        final CompletableFuture<WebSocket> future;

        private BinarySendingTask(ByteBuffer data, boolean last) {
            this.data = data;
            this.last = last;
            this.future = new CompletableFuture<>();
        }
    }

    private CompletableFuture<WebSocket> sendInQueue(BinarySendingTask task) {
        sendingTasks.add(task);
        doSendingTasks();
        return task.future;
    }

    private void doSendingTasks() {
        if (isSending.compareAndSet(false, true)) {
            try {
                BinarySendingTask task;
                CompletableFuture<WebSocket> currentTaskFuture = null;
                while ((task = sendingTasks.poll()) != null) {

                    final BinarySendingTask finalTask = task;

                    if (currentTaskFuture == null) {
                        currentTaskFuture = webSocket.sendBinary(task.data, task.last);
                    } else {
                        currentTaskFuture = currentTaskFuture.thenCompose(websocket -> websocket.sendBinary(finalTask.data, finalTask.last));
                    }

                    currentTaskFuture.whenComplete((webSocket1, throwable) -> {
                        if (throwable != null) {
                            finalTask.future.completeExceptionally(throwable);
                        } else {
                            finalTask.future.complete(webSocket1);
                        }
                    });
                }

                if (currentTaskFuture != null) {
                    currentTaskFuture.whenComplete((webSocket, throwable) -> {
                        isSending.set(false);
                        if (sendingTasks.size() > 0) {
                            doSendingTasks();
                        }
                    });
                } else {
                    isSending.set(false);
                    if (sendingTasks.size() > 0) {
                        doSendingTasks();
                    }
                }
            } catch (Throwable throwable) {
                // it shouldn't throw any exception, consider removing try/catch
                throwable.printStackTrace();
            }
        }
    }

    private void resetStream(Integer requestId, int errorCode, String message) {
        final ByteBuffer rst = rstMessage(requestId, errorCode, message);
        sendBinary(rst, true);
        contextMap.remove(requestId);
    }

    private void putHeadersTo(HttpRequest.Builder requestToTarget, CrankerRequest crankerRequest) {
        for (String line : crankerRequest.headers) {
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
        this.websocketState = State.IDLE;
        webSocket.request(1);
        pingPongTask = executor.scheduleAtFixedRate(() -> {
            try {
                ByteBuffer wrap = ByteBuffer.wrap(PING_MSG);
                webSocket.sendPing(wrap);
            } catch (Exception e) {
                closeWebsocket(State.ERROR, 1011, e);
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
        // V3 protocol not using the onText anymore...
        return null;
    }

    private class BufferHolder {
        final ByteBuffer byteBuffer;
        final CompletableFuture<?> future;
        private BufferHolder(ByteBuffer byteBuffer) {
            this.byteBuffer = byteBuffer;
            this.future = new CompletableFuture();
        }
    }

    @Override
    public CompletionStage<?> onBinary(WebSocket webSocket, ByteBuffer data, boolean last) {

        if (websocketState.isCompleted()) {
            // consume the data on the fly, so that CLOSE frame can arrive and websocket can close gracefully
            webSocket.request(1);
            return null;
        }

        onSignOfLife();

        if (!last) {
            final BufferHolder holder = new BufferHolder(data);
            unCompletedBuffers.add(holder);
            webSocket.request(1);
            return holder.future;
        }

        CompletableFuture<?> releaseByteBuffer = new CompletableFuture<>();;
        ByteBuffer completedData;

        // concatenate previous uncompleted buffers and put them into a new allocated ByteBuffer
        if (unCompletedBuffers.size() > 0) {
            unCompletedBuffers.add(new BufferHolder(data)); // add me
            final int capacity = unCompletedBuffers.stream().mapToInt(item -> item.byteBuffer.remaining()).sum();
            completedData = data.isDirect() ? ByteBuffer.allocateDirect(capacity) : ByteBuffer.allocate(capacity);
            for (BufferHolder holder : unCompletedBuffers) {
                completedData.put(holder.byteBuffer);
                holder.future.complete(null);
            }
            completedData.rewind();
            unCompletedBuffers.clear();
        } else {
            completedData = data;
        }

        final int messageType = completedData.get();
        final int flags = completedData.get();
        final Integer requestId = completedData.getInt();

        switch (messageType) {
            case MESSAGE_TYPE_DATA: {
                final boolean isEnd = ((flags & 1) > 0);
                final int len = completedData.remaining();
                final RequestContext context = contextMap.get(requestId);
                if (context != null) {
                    context.wssReceivedMessageBytes.addAndGet(len);
                    context.pendingRequestBodyTasks.add(subscriber -> {
                        try {
                            if (subscriber == null) {
                                return CompletableFuture.failedFuture(new RuntimeException("Subscriber not exist, requestId=" + requestId));
                            }
                            if (completedData.remaining() > 0) {
                                subscriber.onNext(completedData);
                            }
                            if (isEnd) {
                                subscriber.onComplete();
                            }
                            sendBinary(windowUpdateMessage(requestId, len), true);
                            return CompletableFuture.completedFuture(null);
                        } catch (Throwable throwable) {
                            return CompletableFuture.failedFuture(throwable);
                        } finally {
                            releaseByteBuffer.complete(null);
                        }
                    });
                    context.sendPendingDataMaybe();
                }
                webSocket.request(1);
                break;
            }
            case MESSAGE_TYPE_HEADER: {
                if (State.IDLE.equals(this.websocketState)) {
                    this.websocketState = State.HANDLING_REQUEST;
                }
                final boolean isStreamEnd = ((flags & 1) > 0);
                final boolean isHeaderEnd = ((flags & 4) > 0);
                final RequestContext context = contextMap.computeIfAbsent(requestId, RequestContext::new);
                final int len = completedData.remaining();
                context.wssReceivedMessageBytes.addAndGet(len);
                final String content = StandardCharsets.UTF_8.decode(completedData).toString();
                if (!isHeaderEnd) {
                    if (context.headerLineBuilder == null) context.headerLineBuilder = new StringBuilder();
                    context.headerLineBuilder.append(content);
                } else {
                    String fullContent = content;
                    if (context.headerLineBuilder != null) {
                        context.headerLineBuilder.append(content);
                        fullContent = context.headerLineBuilder.toString();
                    }
                    CrankerRequest protocolRequest = new CrankerRequest(fullContent);
                    newRequestToTarget(context, protocolRequest, webSocket, isStreamEnd);
                }
                sendBinary(windowUpdateMessage(requestId, len), true);
                releaseByteBuffer.complete(null);
                webSocket.request(1);
                break;
            }
            case MESSAGE_TYPE_RST_STREAM: {
                final int errorCode = getErrorCode(completedData);
                final String errorMessage = getErrorMessage(completedData);
                final RequestContext context = contextMap.remove(requestId);
                if (context != null && context.request != null) {
                    context.close();
                    proxyEventListener.onProxyError(context.request, new IllegalStateException(
                        String.format("Received rstMessage from cranker, client may closed request early." +
                            "errorCode=%s, errorMessage=%s", errorCode, errorMessage)));
                }
                releaseByteBuffer.complete(null);
                webSocket.request(1);
                break;
            }
            case MESSAGE_TYPE_WINDOW_UPDATE: {
                final int windowUpdate = completedData.getInt();
                final RequestContext context = contextMap.get(requestId);
                if (context != null) {
                    context.ackedBytes(windowUpdate);
                }
                releaseByteBuffer.complete(null);
                webSocket.request(1);
                break;
            }
            default: {
                // not support types
                releaseByteBuffer.complete(null);
                webSocket.request(1);
                break;
            }
        }

        return releaseByteBuffer;
    }

    private static int getErrorCode(ByteBuffer byteBuffer) {
        return byteBuffer.remaining() >= 4 ? byteBuffer.getInt() : -1;
    }

    private static String getErrorMessage(ByteBuffer byteBuffer) {
        String message = "";
        if (byteBuffer.remaining() > 0) {
            byte[] bytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(bytes);
            message = new String(bytes, StandardCharsets.UTF_8);
        }
        return message;
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
        closeWebsocket(State.ROUTER_CLOSED, WebSocket.NORMAL_CLOSURE, null);
        return null;
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
        closeWebsocket(State.ERROR, 1011, error);
    }

    void closeWebsocket(State newState, int statusCode, Throwable error) {
        updateState(newState);
        cancelTimeout();
        if (pingPongTask != null) {
            pingPongTask.cancel(false);
            pingPongTask = null;
        }
        if (webSocket != null && !webSocket.isOutputClosed()) {
            webSocket.sendClose(statusCode, error != null ? error.getMessage() : "");
        }
        for (RequestContext context : contextMap.values()) {
            context.close();
        }
        contextMap.clear();
        listener.onClose(this, error);
    }

    @Override
    public State state() {
        return websocketState;
    }

    @Override
    public String version() {
        return "cranker_3.0";
    }

    void close() {
        closeWebsocket(State.CONNECTOR_CLOSED, 1001, null);
    }

    /**
     * Get the underlying complete CompletableFuture. This method will not trigger socket close,
     * if you need close the socket manually, use {{@link #closeWebsocket(State, int, Throwable)}}
     * @return CompletableFuture resolved when socket closed
     */
    CompletableFuture<Void> complete() {
        return complete;
    }

    /**
     * Update socket state
     * @param state new state
     */
    public void updateState(State state) {
        this.websocketState = state;
        if (state.isCompleted()) {
            complete.complete(null);
        }
    }

    @Override
    public String toString() {
        return "ConnectorSocket{" +
            "targetURI=" + targetURI +
            ", state=" + websocketState +
            ", requests=" + contextMap.size() +
            '}';
    }

    private class TargetResponseHandlerV3 implements HttpResponse.BodyHandler<Void> {
        private final RequestContext context;
        private final CrankerResponseBuilder protocolResponse;
        private final WebSocket webSocket;

        public TargetResponseHandlerV3(RequestContext context, CrankerResponseBuilder protocolResponse, WebSocket webSocket) {
            this.context = context;
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

            String headerText = protocolResponse.build();

            // divided the header into smaller chunk to avoid continuation frame
            int headerByte = 0;
            final ByteBuffer[] headerMessages = headerMessages(context.requestId, true, false, headerText);
            headerByte += (headerMessages[0].remaining() - 6);
            CompletableFuture<WebSocket> headerFuture = sendBinary(headerMessages[0], true);

            for (int i = 1; i < headerMessages.length; i++) {
                final ByteBuffer headerMessage = headerMessages[i];
                headerByte += (headerMessage.remaining() - 6);
                headerFuture = headerFuture.thenCompose(websocket -> sendBinary(headerMessage, true));
            }

            int finalHeaderByte = headerByte;
            context.sendingBytes(finalHeaderByte);
            headerFuture.whenComplete((webSocket1, throwable) -> {
                if (throwable != null) {
                    resetStream(context.requestId, 1011, "failed to send text message: " + throwable.getMessage());
                } else {
                    context.sentBytes(finalHeaderByte);
                }
            });

            CompletableFuture<WebSocket> finalHeaderFuture = headerFuture;
            return HttpResponse.BodySubscribers.fromSubscriber(new Flow.Subscriber<>() {

                private Flow.Subscription subscription;

                @Override
                public void onSubscribe(Flow.Subscription subscription) {
                    this.subscription = subscription;
                    context.responseBodySubscription = subscription;
                    finalHeaderFuture.whenComplete((ws, throwable) -> {
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

                    if (webSocket.isOutputClosed()) {
                        subscription.cancel();
                        onError(new RuntimeException("Error sending response body, output channel is closed"));
                        return;
                    }

                    if (!contextMap.containsKey(context.requestId)) {
                        subscription.cancel();
                        onError(new RuntimeException("client close early"));
                        return;
                    }

                    int bodyBytes = 0;
                    CompletableFuture<WebSocket> last = CompletableFuture.completedFuture(webSocket);
                    for (ByteBuffer item : items) {
                        final ByteBuffer data = dataMessages(context.requestId, false, item);
                        bodyBytes += (data.remaining() - 6);
                        last = sendBinary(data, true);
                    }

                    int finalBodyBytes = bodyBytes;
                    context.sendingBytes(finalBodyBytes);
                    last.whenComplete((ws, error) -> {
                        if (error != null) {
                            subscription.cancel();
                            onError(error);
                        } else {
                            context.sentBytes(finalBodyBytes);
                            context.flowControl(() -> subscription.request(1));
                        }
                    });
                }

                @Override
                public void onError(Throwable throwable) {
                    resetStream(context.requestId, 1011, "target response body receiving error: " + throwable.getMessage());
                }

                @Override
                public void onComplete() {
                    // indicate that it doesn't need to be cleaned on exception or error
                    context.responseBodySubscription = null;

                    sendBinary(dataMessages(context.requestId, true, null), true);
                    contextMap.remove(context.requestId);
                    // for graceful shutdown
                    if (State.STOPPING.equals(state()) && contextMap.isEmpty()) {
                        closeWebsocket(State.COMPLETE, WebSocket.NORMAL_CLOSURE, null);
                    }
                }
            });
        }
    }

    static ByteBuffer windowUpdateMessage(Integer requestId, Integer windowUpdate) {
        return ByteBuffer.allocate(10)
            .put(MESSAGE_TYPE_WINDOW_UPDATE) // 1 byte
            .put((byte) 0) // 1 byte, flags unused
            .putInt(requestId) // 4 byte
            .putInt(windowUpdate) // 4 byte
            .rewind();
    }

    static ByteBuffer rstMessage(Integer requestId, Integer errorCode, String message) {
        final byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(10 + bytes.length)
            .put(MESSAGE_TYPE_RST_STREAM) // 1 byte
            .put((byte) 0) // 1 byte, flags unused
            .putInt(requestId) // 4 byte
            .putInt(errorCode) // 4 byte
            .put(bytes)
            .rewind();
    }

    static ByteBuffer[] headerMessages(Integer requestId, boolean isHeaderEnd, boolean isStreamEnd, String fullHeaderLine) {
        final int chunkSize = 16000;
        if (fullHeaderLine.length() < chunkSize) {
            return new ByteBuffer[]{headerMessage(requestId, isHeaderEnd, isStreamEnd, fullHeaderLine)};
        }

        List<ByteBuffer> buffers = new ArrayList<>();
        for (int i = 0; i < fullHeaderLine.length(); i += chunkSize) {
            final int endIndex = Math.min(fullHeaderLine.length(), i + chunkSize);
            final boolean isLast = endIndex == fullHeaderLine.length();
            buffers.add(headerMessage(requestId, isLast, isStreamEnd, fullHeaderLine.substring(i, endIndex)));
        }
        return buffers.toArray(new ByteBuffer[0]);
    }

    static ByteBuffer headerMessage(Integer requestId, boolean isHeaderEnd, boolean isStreamEnd, String headerLine) {
        int flags = 0;
        if (isStreamEnd) flags = flags | 1; // first bit 00000001
        if (isHeaderEnd) flags = flags | 4; // third bit 00000100
        final byte[] bytes = headerLine.getBytes(StandardCharsets.UTF_8);
        final ByteBuffer message = ByteBuffer.allocate(6 + bytes.length)
            .put(MESSAGE_TYPE_HEADER) // 1 byte
            .put((byte) flags) // 1 byte
            .putInt(requestId) // 4 byte
            .put(bytes)
            .rewind();
        return message;
    }

    static ByteBuffer dataMessages(Integer requestId, boolean isEnd, ByteBuffer buffer) {
        final ByteBuffer message = ByteBuffer.allocate(6 + (buffer == null ? 0 : buffer.remaining()))
            .put(MESSAGE_TYPE_DATA) // 1 byte
            .put((byte) (isEnd ? 1 : 0)) // 1 byte
            .putInt(requestId); // 4 byte
        if (buffer != null) message.put(buffer);
        message.rewind();
        return message;
    }

    private class TargetRequestBodyPublisher implements HttpRequest.BodyPublisher {
        private final CrankerRequest protocolRequest;
        private final WebSocket webSocket;
        private final RequestContext context;

        public TargetRequestBodyPublisher(RequestContext context, CrankerRequest protocolRequest, WebSocket webSocket) {
            this.context = context;
            this.protocolRequest = protocolRequest;
            this.webSocket = webSocket;
        }

        @Override
        public long contentLength() {
            return protocolRequest.bodyLength();
        }

        @Override
        public void subscribe(Flow.Subscriber<? super ByteBuffer> subscriber) {
            context.requestBodySubscriber = subscriber;

            subscriber.onSubscribe(new Flow.Subscription() {
                @Override
                public void request(long n) {
                    context.requestCount.addAndGet(n);
                    context.sendPendingDataMaybe();
                }

                @Override
                public void cancel() {
                    resetStream(context.requestId, 1011, "target request body cancelled.");
                }
            });
        }
    }

    private class CrankerRequest {

        public String httpMethod;
        public String dest;
        public String[] headers;

        public CrankerRequest(CharSequence msg) {
            String[] msgArr = msg.toString().split("\n");
            String request = msgArr[0];
            String[] bits = request.split(" ");
            this.httpMethod = bits[0];
            this.dest = bits[1];
            this.headers = Arrays.copyOfRange(msgArr, 1, msgArr.length);
        }

        public long bodyLength() {
            for (String headerLine : headers) {
                // line sample: "Content-Length:100000"
                if (headerLine.toLowerCase().startsWith("content-length:")) {
                    String[] split = headerLine.split(":");
                    if (split.length == 2) {
                        return Long.parseLong(split[1].trim());
                    }
                }
            }
            return -1;
        }
    }

    private class RequestContext {

        final private static int WATER_MARK_HIGH = 64 * 1024;
        final private static int WATER_MARK_LOW = 16 * 1024;

        // wss tunnel
        final private AtomicInteger wssReceivedMessageBytes = new AtomicInteger(0);
        final private AtomicInteger wssReceivedAckBytes = new AtomicInteger(0);
        final private AtomicInteger wssSentBytes = new AtomicInteger(0);
        final private AtomicInteger wssSendingBytes = new AtomicInteger(0);
        final private AtomicBoolean isWssWritable = new AtomicBoolean(true);
        final private AtomicBoolean isWssWriting = new AtomicBoolean(false);
        final private Queue<Runnable> wssWriteCallbacks = new ConcurrentLinkedQueue<>();

        final Integer requestId;
        final long startTimeMillis;

        // client request/response
        HttpRequest request;
        CompletableFuture<HttpResponse<Void>> responseFuture;
        Flow.Subscription responseBodySubscription;
        StringBuilder headerLineBuilder;
        Flow.Subscriber<? super ByteBuffer> requestBodySubscriber;

        ConcurrentLinkedQueue<Function<Flow.Subscriber<? super ByteBuffer>, CompletableFuture<Void>>> pendingRequestBodyTasks;

        AtomicBoolean isSending = new AtomicBoolean(false);
        AtomicLong requestCount = new AtomicLong(0);

        private RequestContext(Integer requestId) {
            this.requestId = requestId;
            this.startTimeMillis = System.currentTimeMillis();
            this.pendingRequestBodyTasks = new ConcurrentLinkedQueue<>();
        }

        void sendPendingDataMaybe() {
            if (requestCount.get() > 0
                && pendingRequestBodyTasks.size() > 0
                && requestBodySubscriber != null
                && contextMap.containsKey(requestId)
                && isSending.compareAndSet(false, true)) {
                try {

                    CompletableFuture<Void> latest = CompletableFuture.completedFuture(null);

                    while (requestCount.get() > 0
                        && pendingRequestBodyTasks.size() > 0
                        && requestBodySubscriber != null
                        && contextMap.containsKey(requestId)) {

                        final Function<Flow.Subscriber<? super ByteBuffer>, CompletableFuture<Void>> current = pendingRequestBodyTasks.poll();
                        requestCount.decrementAndGet();
                        latest = latest.thenCompose((Void) -> current.apply(requestBodySubscriber));

                    }

                    latest.whenComplete((voi, error) -> {
                        if (error != null) {
                            onError(error);
                        }
                    });

                } catch (Throwable throwable) {
                    onError(throwable);
                } finally {
                    isSending.set(false);
                    sendPendingDataMaybe();
                }
            }
        }

        private void onError(Throwable error) {
            if (contextMap.containsKey(requestId)) {
                resetStream(requestId, 1011, "request body sending failed: " + error.getMessage());
                pendingRequestBodyTasks.clear();
            }
        }

        void sentBytes(int send) {
            this.wssSentBytes.addAndGet(send);
        }

        void sendingBytes(int sendingBytes) {
            this.wssSendingBytes.addAndGet(sendingBytes);
            if (this.wssSendingBytes.get() > WATER_MARK_HIGH) {
                isWssWritable.compareAndSet(true, false);
            }
        }

        void ackedBytes(int ack) {
            this.wssReceivedAckBytes.addAndGet(ack);
            this.wssSendingBytes.addAndGet(-ack);
            if (wssSendingBytes.get() < WATER_MARK_LOW) {
                if (isWssWritable.compareAndSet(false, true)) {
                    // Websocket.Listener.onBinary() is running under HttpClient-n-SelectorManager thread
                    // It randomly throw exception when underlying call invoking flush on the websocket, or hang up.
                    // so using another thread here to avoid the failure
                    executor.submit(this::writeItMaybe);
                }
            }
        }

        void flowControl(Runnable runnable) {
            if (isWssWritable.get() && !isWssWriting.get()) {
                runnable.run();
            } else {
                wssWriteCallbacks.add(runnable);
                writeItMaybe();
            }
        }

        private void writeItMaybe() {
            if (isWssWritable.get() && wssWriteCallbacks.size() > 0 && isWssWriting.compareAndSet(false, true)) {
                try {
                    Runnable current;
                    while (isWssWritable.get() && (current = wssWriteCallbacks.poll()) != null) {
                        current.run();
                    }
                } finally {
                    isWssWriting.set(false);
                    writeItMaybe();
                }
            }
        }

        void close() {
            if (responseFuture != null && !responseFuture.isDone() && !responseFuture.isCancelled()) {
                responseFuture.cancel(true);
            }
            if (responseBodySubscription != null) {
                responseBodySubscription.cancel();
            }
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", RequestContext.class.getSimpleName() + "[", "]")
                .add("wssReceivedMessageBytes=" + wssReceivedMessageBytes)
                .add("wssSentBytes=" + wssSentBytes)
                .add("wssReceivedAckBytes=" + wssReceivedAckBytes)
                .add("wssSendingBytes=" + wssSendingBytes)
                .add("isWssWritable=" + isWssWritable)
                .add("wssWriteCallbacks=" + wssWriteCallbacks.size())
                .add("isWssWriting=" + isWssWriting)
                .add("requestId=" + requestId)
                .add("startTimeMillis=" + startTimeMillis)
                .add("request=" + request)
                .add("pendingRequestBodyTasks=" + pendingRequestBodyTasks.size())
                .add("isSending=" + isSending)
                .add("requestCount=" + requestCount)
                .toString();
        }
    }
}
