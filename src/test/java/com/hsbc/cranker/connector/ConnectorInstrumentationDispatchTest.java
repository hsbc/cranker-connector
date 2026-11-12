package com.hsbc.cranker.connector;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ConnectorInstrumentationDispatchTest {
    @ParameterizedTest
    @CsvSource({
        "cranker_1.0, proxy", "cranker_3.0, proxy",
        "cranker_1.0, send", "cranker_3.0, send",
        "cranker_1.0, completed-success", "cranker_3.0, completed-success",
        "cranker_1.0, completed-failure", "cranker_3.0, completed-failure"
    })
    void scopesRestorePreviousContextOnSynchronousExit(String protocol, String outcome) throws Exception {
        // Invoke the socket callback directly: assert context lifetime, not HTTP error or tunnel recovery policy.
        ThreadLocal<String> current = new ThreadLocal<>();
        current.set("previous");
        List<String> events = new ArrayList<>();
        Thread dispatchThread = Thread.currentThread();
        IllegalStateException failure = new IllegalStateException("test dispatch failure");
        HttpRequest[] returned = new HttpRequest[1];
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        InstrumentationTestLog log = new InstrumentationTestLog();
        ConnectorSocketAdapter socket = null;
        try {
            HttpClient client = new DelegatingHttpClient(HttpClient.newHttpClient()) {
                @Override
                public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request, HttpResponse.BodyHandler<T> handler) {
                    assertEquals("inner", current.get());
                    assertSame(returned[0], request);
                    events.add("send");
                    if ("send".equals(outcome)) throw failure;
                    // Only model submission/completion-handler installation, not the response body lifecycle.
                    return "completed-failure".equals(outcome)
                        ? CompletableFuture.failedFuture(failure) : CompletableFuture.completedFuture(null);
                }
            };
            ProxyEventListener proxy = new ProxyEventListener() {
                @Override
                public HttpRequest beforeProxyToTarget(HttpRequest request, HttpRequest.Builder builder) {
                    assertEquals("inner", current.get());
                    events.add("proxy");
                    if ("proxy".equals(outcome)) throw failure;
                    returned[0] = request;
                    return request;
                }
            };
            ConnectorInstrumentation instrumentation = new ConnectorInstrumentation(List.of(
                scope("outer", "previous", current, dispatchThread, events),
                scope("inner", "outer", current, dispatchThread, events)));
            ConnectorSocketListener listener = new ConnectorSocketListener() {
                @Override public void onConnectionAcquired(ConnectorSocket socket) { }
                @Override public void onClose(ConnectorSocket socket, Throwable error) { }
            };
            socket = new ConnectorSocketAdapter(URI.create("http://localhost:8080"), client, listener,
                proxy, executor, instrumentation);
            WebSocket webSocket = new ImmediateWebSocket(protocol);
            socket.onOpen(webSocket);
            ConnectorSocketAdapter dispatchSocket = socket;
            if ("proxy".equals(outcome) || "send".equals(outcome)) {
                assertSame(failure, assertThrows(IllegalStateException.class,
                    () -> dispatch(dispatchSocket, webSocket, protocol)));
            } else {
                assertDoesNotThrow(() -> dispatch(dispatchSocket, webSocket, protocol));
            }
            assertEquals("previous", current.get());
            List<String> expected = new ArrayList<>(List.of("open outer", "open inner", "proxy"));
            if (!"proxy".equals(outcome)) expected.add("send");
            expected.addAll(List.of("close inner", "close outer"));
            assertEquals(expected, events);
            assertTrue(log.failures.isEmpty(), log.failures.toString());
        } finally {
            try {
                if (socket != null) socket.close();
            } finally {
                current.remove();
                log.close();
                executor.shutdownNow();
                assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }

    private static ConnectorInstrumentationListener scope(String name, String previous, ThreadLocal<String> current,
                                                           Thread thread, List<String> events) {
        return new ConnectorInstrumentationListener() {
            @Override
            public Runnable beforeTargetDispatch(TargetDispatchInfo info) {
                assertSame(thread, Thread.currentThread());
                assertEquals(previous, current.get());
                current.set(name);
                events.add("open " + name);
                return () -> {
                    try {
                        assertSame(thread, Thread.currentThread());
                        assertEquals(name, current.get());
                    } finally {
                        current.set(previous);
                        events.add("close " + name);
                    }
                };
            }
        };
    }

    private static void dispatch(ConnectorSocketAdapter socket, WebSocket webSocket, String protocol) {
        String headers = "GET /test HTTP/1.1\nx-test: value\n\n";
        if ("cranker_1.0".equals(protocol)) {
            socket.onText(webSocket, headers + CrankerRequestParser.REQUEST_HAS_NO_BODY_MARKER, true);
        } else {
            socket.onBinary(webSocket, ConnectorSocketV3.headerMessage(1, true, true, headers), true);
        }
    }

    private static class ImmediateWebSocket implements WebSocket {
        private final String protocol;

        ImmediateWebSocket(String protocol) { this.protocol = protocol; }

        @Override public CompletableFuture<WebSocket> sendText(CharSequence data, boolean last) { return sent(); }
        @Override public CompletableFuture<WebSocket> sendBinary(ByteBuffer data, boolean last) { return sent(); }
        @Override public CompletableFuture<WebSocket> sendPing(ByteBuffer message) { return sent(); }
        @Override public CompletableFuture<WebSocket> sendPong(ByteBuffer message) { return sent(); }
        @Override public CompletableFuture<WebSocket> sendClose(int statusCode, String reason) { return sent(); }
        @Override public void request(long n) { }
        @Override public String getSubprotocol() { return protocol; }
        @Override public boolean isOutputClosed() { return false; }
        @Override public boolean isInputClosed() { return false; }
        @Override public void abort() { }

        private CompletableFuture<WebSocket> sent() { return CompletableFuture.completedFuture(this); }
    }
}
