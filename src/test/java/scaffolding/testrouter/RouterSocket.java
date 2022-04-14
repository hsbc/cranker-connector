package scaffolding.testrouter;

import io.muserver.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.WebApplicationException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static scaffolding.testrouter.CrankerMuHandler.HOP_BY_HOP;


class RouterSocket extends BaseWebSocket implements ProxyInfo {
    private static final Logger log = LoggerFactory.getLogger(RouterSocket.class);
    private static final List<String> RESPONSE_HEADERS_TO_NOT_SEND_BACK = Collections.singletonList("server");

    final String route;
    final String routerSocketID = UUID.randomUUID().toString();
    private final WebSocketFarm webSocketFarm;
    private final String connectorInstanceID;
    private final List<ProxyListener> proxyListeners;
    private Runnable onReadyForAction;
    private InetSocketAddress remoteAddress;
    private boolean isRemoved;
    private boolean hasResponse;
    private long requestStartTime = 0;
    private final AtomicLong bytesReceived = new AtomicLong();
    private final AtomicLong bytesSent = new AtomicLong();
    private AsyncHandle asyncHandle;
    private MuResponse response;
    private MuRequest clientRequest;
    private int socketAttempts;
    private Throwable error;
    private long durationMillis = 0;

    RouterSocket(String route, WebSocketFarm webSocketFarm, String remotePort, List<ProxyListener> proxyListeners) {
        this.webSocketFarm = webSocketFarm;
        this.route = route;
        this.connectorInstanceID = remotePort;
        this.proxyListeners = proxyListeners;
        this.isRemoved = false;
        this.hasResponse = false;
    }

    @Override
    public boolean isCatchAll() {
        return "*".equals(route);
    }

    void socketSessionClose() {
        try {
            MuWebSocketSession session = session();
            if (session != null) {
                session.close();
            }
        } catch (Exception ignored) {
        }
    }

    @Override
    public void onConnect(MuWebSocketSession session) throws Exception {
        super.onConnect(session);
        this.remoteAddress = session.remoteAddress();
        onReadyForAction.run();
    }

    @Override
    public void onClientClosed(int statusCode, String reason) throws Exception {
        super.onClientClosed(statusCode, reason);
        try {
            if (hasResponse) {
                if (statusCode == 1011) {
                    response.status(502);
                } else if (statusCode == 1008) {
                    response.status(400);
                }
            }
            if (asyncHandle != null) {
                try {
                    asyncHandle.complete();
                } catch (IllegalStateException e) {
                    log.info("Tried to complete a request, but it is probably already closed. " +
                        " routerName=" + route +
                        ", routerSocketID=" + routerSocketID, e);
                }
            }

            if (!isRemoved) {
                webSocketFarm.removeWebSocket(route, this);
                isRemoved = true;
            }
        } finally {
            raiseCompletionEvent();
        }
    }

    private void raiseCompletionEvent() {
        if (clientRequest != null && !proxyListeners.isEmpty()) {
            durationMillis = System.currentTimeMillis() - requestStartTime;
            for (ProxyListener completionListener : proxyListeners) {
                try {
                    completionListener.onComplete(this);
                } catch (Exception e) {
                    log.warn("Error thrown by " + completionListener, e);
                }
            }
        }
    }

    @Override
    public void onError(Throwable cause) throws Exception {
        try {
            this.error = cause;
            super.onError(cause);
            removeBadWebSocket();
            boolean shouldCancelClientRequest = asyncHandle != null;
            if (cause instanceof TimeoutException) {
                if (response != null && !response.hasStartedSendingData()) {
                    shouldCancelClientRequest = false;
                    String htmlBody = "The <code>" + Mutils.htmlEncode(route) + "</code> service did not respond in time.";
                    CrankerMuHandler.sendSimpleResponse(response, asyncHandle, 504, "504 Gateway Timeout", htmlBody);
                }
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("WebSocketError, routerName=" + route + ", routerSocketID=" + routerSocketID, cause);
                }
            }
            if (shouldCancelClientRequest) {
                asyncHandle.complete(cause);
            }
        } finally {
            raiseCompletionEvent();
        }
    }

    @Override
    public void onText(String message, boolean isLast, DoneCallback doneCallback) throws Exception {
        if (hasResponse) {
            CrankerProtocolResponse protocolResponse = new CrankerProtocolResponse(message);
            response.status(protocolResponse.getStatus());
            putHeadersTo(protocolResponse);

            try {
                if (!proxyListeners.isEmpty()) {
                    for (ProxyListener proxyListener : proxyListeners) {
                        proxyListener.onBeforeRespondingToClient(this);
                    }
                }
            } catch (WebApplicationException e) {
                CrankerMuHandler.handleWebApplicationException(e, response, asyncHandle);
            }

            bytesSent.getAndAdd(message.length()); // string length should be number of bytes as this is used for headers so is ASCII
        }
        doneCallback.onComplete(null);
    }

    @Override
    public void onBinary(ByteBuffer byteBuffer, boolean isLast, DoneCallback doneCallback) throws Exception {
        int len = byteBuffer.remaining();
        if (len == 0) {
            log.warn("routerName=" + route + ", routerSocketID=" + routerSocketID +
                ", received 0 bytes to send to " + remoteAddress + " - " + response);
            doneCallback.onComplete(null);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("routerName=" + route + ", routerSocketID=" + routerSocketID +
                    ", sending " + len + " bytes to client");
            }
            asyncHandle.write(byteBuffer, errorIfAny -> {
                if (errorIfAny == null) {
                    bytesSent.addAndGet(len);
                } else {
                    log.info("routerName=" + route + ", routerSocketID=" + routerSocketID +
                        ", could not write to client response (maybe the user closed their browser)" +
                        " so will cancel the request. Error message: " + errorIfAny.getMessage());
                }
                doneCallback.onComplete(errorIfAny); // if error not null, then onError will be called
            });
        }
    }

    void sendText(String message) {
        bytesReceived.getAndAdd(message.length()); // string length should be number of bytes as this is used for headers so is ASCII
        session().sendText(message, error1 -> {}); // TODO: close the client here on error?
    }

    void sendData(ByteBuffer bb, DoneCallback callback) {
        bytesReceived.getAndAdd(bb.remaining());
        session().sendBinary(bb, callback); // TODO: close the client here?
    }

    private void removeBadWebSocket() {
        if (!isRemoved) {
            socketSessionClose();
            webSocketFarm.removeWebSocket(route, this);
            isRemoved = true;
        }
    }

    @Override
    public String connectorInstanceID() {
        return connectorInstanceID;
    }

    void setOnReadyForAction(Runnable onReadyForAction) {
        this.onReadyForAction = onReadyForAction;
    }

    @Override
    public InetSocketAddress serviceAddress() {
        return remoteAddress;
    }

    private void putHeadersTo(CrankerProtocolResponse protocolResponse) {
        response.headers().remove("date"); // Remove cranker-router's date because we want to use the target server's date
        for (String line : protocolResponse.headers) {
            int pos = line.indexOf(':');
            if (pos > 0) {
                String header = line.substring(0, pos);
                String lowerHeader = header.toLowerCase();
                if (!HOP_BY_HOP.contains(lowerHeader) && !RESPONSE_HEADERS_TO_NOT_SEND_BACK.contains(lowerHeader)) {
                    String value = line.substring(pos + 1);
                    response.headers().add(lowerHeader, value);
                }
            }
        }

        List<String> customHopByHop = CrankerMuHandler.getCustomHopByHopHeaders(response.headers().get(HeaderNames.CONNECTION));
        for (String header : customHopByHop) {
            response.headers().remove(header);
        }

    }

    void setAsyncHandle(AsyncHandle asyncHandle, MuRequest clientRequest, MuResponse response, int socketAttempts) {
        this.clientRequest = clientRequest;
        this.socketAttempts = socketAttempts;
        this.hasResponse = true;
        this.response = response;
        this.requestStartTime = System.currentTimeMillis();
        this.asyncHandle = asyncHandle;
    }

    boolean isDarkModeOn(Set<DarkHost> darkHosts) {
        InetAddress thisSocketAddress = serviceAddress().getAddress();
        for (DarkHost darkHost : darkHosts) {
            if (darkHost.sameHost(thisSocketAddress)) {
                return true;
            }
        }
        return false;
    }


    @Override
    public String route() {
        return route;
    }

    @Override
    public MuRequest request() {
        return clientRequest;
    }

    @Override
    public MuResponse response() {
        return response;
    }

    @Override
    public long durationMillis() {
        return durationMillis;
    }

    @Override
    public long bytesReceived() {
        return bytesReceived.get();
    }

    @Override
    public long bytesSent() {
        return bytesSent.get();
    }

    @Override
    public Throwable errorIfAny() {
        return error;
    }

    @Override
    public int socketAttempts() {
        return socketAttempts;
    }
}
