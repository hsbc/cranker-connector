package com.hsbc.cranker.connector;

import com.hsbc.cranker.mucranker.CrankerRouter;
import io.muserver.MuServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.hsbc.cranker.mucranker.CrankerRouterBuilder.crankerRouter;
import static io.muserver.MuServerBuilder.httpServer;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;
import static scaffolding.AssertUtils.assertEventually;

class CrankerConnectorDispatchTest {
    private final InstrumentationTestLog instrumentationLog = new InstrumentationTestLog();
    private CrankerRouter router;
    private MuServer routerServer;
    private MuServer targetServer;
    private final List<CrankerConnector> connectors = new ArrayList<>();
    private final ExecutorService clientExecutor = Executors.newFixedThreadPool(8);
    private final CountDownLatch allowResponses = new CountDownLatch(1);

    @AfterEach
    void stop() throws InterruptedException {
        try {
            stopResources();
            assertTrue(instrumentationLog.failures.isEmpty(), instrumentationLog.failures.toString());
        } finally {
            instrumentationLog.close();
        }
    }

    private void stopResources() throws InterruptedException {
        allowResponses.countDown();
        try {
            assertAll(connectors.stream().map(connector ->
                () -> assertTrue(connector.stop(10, TimeUnit.SECONDS))));
        } finally {
            try {
                if (targetServer != null) targetServer.stop();
            } finally {
                try {
                    if (routerServer != null) routerServer.stop();
                } finally {
                    if (router != null) router.stop();
                    clientExecutor.shutdownNow();
                    assertTrue(clientExecutor.awaitTermination(10, TimeUnit.SECONDS));
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"cranker_1.0", "cranker_3.0"})
    void scopesCoverApplicationCallbacksAndRealClientDispatchNotResponses(String protocol) throws Exception {
        int count = 8;
        CountDownLatch closed = new CountDownLatch(count);
        CountDownLatch followUpClosed = new CountDownLatch(1);
        ThreadLocal<String> current = new ThreadLocal<>();
        Map<String, List<String>> events = new ConcurrentHashMap<>();
        HttpClient realClient = HttpClient.newBuilder().executor(clientExecutor)
            .connectTimeout(Duration.ofSeconds(5))
            .followRedirects(HttpClient.Redirect.NEVER)
            .version(HttpClient.Version.HTTP_1_1).build();
        HttpClient dispatchClient = new DelegatingHttpClient(realClient) {
            @Override
            public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request, HttpResponse.BodyHandler<T> handler) {
                String id = request.headers().firstValue("x-id").orElseThrow();
                assertEquals(id + "/inner", current.get());
                assertEquals(id, request.headers().firstValue("x-first").orElseThrow());
                assertEquals(id, request.headers().firstValue("x-second").orElseThrow());
                events.get(id).add("send");
                return super.sendAsync(request, handler);
            }
        };
        targetServer = httpServer().withInterface("127.0.0.1")
            .addHandler((request, response) -> {
                String id = request.headers().get("x-id");
                String body = request.readBodyAsString();
                assertEquals(id, request.headers().get("x-first"));
                assertEquals(id, request.headers().get("x-second"));
                assertTrue(allowResponses.await(20, TimeUnit.SECONDS));
                response.write(id + ":" + body);
                return true;
            }).start();
        router = crankerRouter().withSupportedCrankerProtocols(List.of(protocol)).start();
        routerServer = httpServer().withInterface("127.0.0.1")
            .addHandler(router.createRegistrationHandler()).addHandler(router.createHttpHandler()).start();

        Map<String, HttpRequest> inspected = new ConcurrentHashMap<>();
        ConnectorInstrumentationListener outer = new ConnectorInstrumentationListener() {
            @Override
            public Runnable beforeTargetDispatch(TargetDispatchInfo info) {
                HttpHeaders headers = info.request().headers();
                String id = headers.firstValue("X-ID").orElseThrow();
                assertNull(current.get(), "previous dispatch context leaked");
                assertEquals(List.of("one", "two"), headers.allValues("X-Repeated"));
                assertEquals(List.of("user-value"), headers.allValues("x-general-context"));
                assertFalse(headers.firstValue("content-length").isPresent());
                assertNull(inspected.putIfAbsent(id, info.request()));
                assertNull(events.putIfAbsent(id, new CopyOnWriteArrayList<>()));
                events.get(id).add("open outer");
                Thread thread = Thread.currentThread();
                current.set(id + "/outer");
                return () -> {
                    assertSame(thread, Thread.currentThread());
                    assertEquals(id + "/outer", current.get());
                    current.remove();
                    events.get(id).add("close outer");
                    closed.countDown();
                    if ("follow-up".equals(id)) followUpClosed.countDown();
                };
            }
        };
        ConnectorInstrumentationListener inner = new ConnectorInstrumentationListener() {
            @Override
            public Runnable beforeTargetDispatch(TargetDispatchInfo info) {
                String id = info.request().headers().firstValue("x-id").orElseThrow();
                assertSame(inspected.get(id), info.request());
                assertEquals(id + "/outer", current.get());
                events.get(id).add("open inner");
                Thread thread = Thread.currentThread();
                current.set(id + "/inner");
                return () -> {
                    assertSame(thread, Thread.currentThread());
                    assertEquals(id + "/inner", current.get());
                    current.set(id + "/outer");
                    events.get(id).add("close inner");
                };
            }
        };
        ProxyEventListener application = new ProxyEventListener() {
            @Override
            public HttpRequest beforeProxyToTarget(HttpRequest request, HttpRequest.Builder builder) {
                String id = request.headers().firstValue("x-id").orElseThrow();
                assertEquals(id + "/inner", current.get());
                assertSame(inspected.get(id), request);
                assertFalse(request.headers().firstValue("x-first").isPresent());
                events.get(id).add("before proxy");
                return builder.header("x-first", id).header("x-second", id).build();
            }
        };
        CrankerConnectorBuilder builder = CrankerConnectorBuilder.connector()
            .withTarget(targetServer.uri()).withRoute("*").withHttpClient(dispatchClient)
            .withPreferredProtocols(List.of(protocol))
            .withRouterUris(RegistrationUriSuppliers.fixedUris(List.of(
                BaseEndToEndTest.registrationUri(routerServer.uri()))))
            .withSlidingWindowSize(2)
            .withProxyEventListener(new ProxyEventListener() {})
            .addInstrumentationListener(null, new ConnectorInstrumentationListener() {})
            .addInstrumentationListener("context", outer)
            .addInstrumentationListener(null, inner)
            .withProxyEventListener(application)
            .addInstrumentationListener("context", new ConnectorInstrumentationListener() {
                @Override
                public Runnable beforeTargetDispatch(TargetDispatchInfo info) {
                    throw new AssertionError("duplicate keys must retain their original listener");
                }
            });
        CrankerConnector connector = builder.build();
        connectors.add(connector);
        builder.addInstrumentationListener(null, new ConnectorInstrumentationListener() {
            @Override
            public Runnable beforeTargetDispatch(TargetDispatchInfo info) {
                throw new AssertionError("builder changes must not alter a built connector");
            }
        }).withProxyEventListener(new ProxyEventListener() {
            @Override
            public HttpRequest beforeProxyToTarget(HttpRequest request, HttpRequest.Builder builder) {
                throw new AssertionError("built connector must retain its original proxy listener");
            }
        });
        connector.start();
        BaseEndToEndTest.waitForRegistration("*", connector.connectorId(), 2, router);
        assertEventually(() -> connector.routers().get(0).idleSockets().stream()
            .filter(socket -> protocol.equals(socket.version())).count(), is(2L));

        List<CompletableFuture<HttpResponse<String>>> responses = new ArrayList<>();
        try {
            for (int i = 0; i < count; i++) {
                responses.add(realClient.sendAsync(request("request-" + i), HttpResponse.BodyHandlers.ofString()));
            }
            assertTrue(closed.await(15, TimeUnit.SECONDS), "dispatch scopes did not close");
            for (CompletableFuture<?> response : responses) {
                assertFalse(response.isDone(), "the target has not been allowed to respond yet");
            }
        } finally {
            allowResponses.countDown();
        }
        for (int i = 0; i < responses.size(); i++) {
            HttpResponse<String> response = responses.get(i).get(10, TimeUnit.SECONDS);
            assertEquals(200, response.statusCode());
            assertEquals("request-" + i + ":body-request-" + i, response.body());
        }
        HttpResponse<String> followUp = realClient.send(request("follow-up"), HttpResponse.BodyHandlers.ofString());
        assertEquals(200, followUp.statusCode());
        assertEquals("follow-up:body-follow-up", followUp.body());
        assertTrue(followUpClosed.await(10, TimeUnit.SECONDS), "follow-up scope did not close");
        assertEquals(count + 1, events.size());
        for (List<String> invocation : events.values()) {
            assertEquals(List.of("open outer", "open inner", "before proxy", "send",
                "close inner", "close outer"), invocation);
        }
        assertNull(current.get());
    }

    @ParameterizedTest
    @ValueSource(strings = {"cranker_1.0", "cranker_3.0"})
    void keyedAndNullableRegistrationsKeepOrderAndBuildSnapshots(String protocol) throws Exception {
        HttpClient client = startRegistrationFixture(protocol);
        List<String> calls = new CopyOnWriteArrayList<>();
        ConnectorInstrumentationListener repeated = recording("same", calls);
        CrankerConnectorBuilder builder = registrationBuilder(protocol, client);
        assertSame(builder, builder.addInstrumentationListener(null, repeated));
        assertSame(builder, builder.addInstrumentationListener("key", recording("key", calls)));
        builder.addInstrumentationListener(null, repeated);
        startRegistration(builder, "first", protocol);
        builder.addInstrumentationListener("key", recording("ignored", calls));
        startRegistration(builder, "rebuilt", protocol);
        builder.addInstrumentationListener(null, recording("later", calls));
        startRegistration(builder, "updated", protocol);
        startRegistration(registrationBuilder(protocol, client)
            .addInstrumentationListener("key", recording("independent", calls)), "independent", protocol);

        for (String route : List.of("first", "rebuilt")) {
            assertForwarded(client, route, calls,
                List.of("same", "key", "same", "close same", "close key", "close same"), List.of());
        }
        assertForwarded(client, "updated", calls,
            List.of("same", "key", "same", "later", "close later", "close same", "close key", "close same"), List.of());
        assertForwarded(client, "independent", calls, List.of("independent", "close independent"), List.of());
    }

    @ParameterizedTest
    @ValueSource(strings = {"cranker_1.0", "cranker_3.0"})
    void proxyReplacementAndNullResetDoNotChangeInstrumentationOrKeys(String protocol) throws Exception {
        HttpClient client = startRegistrationFixture(protocol);
        List<String> calls = new CopyOnWriteArrayList<>();
        CrankerConnectorBuilder builder = registrationBuilder(protocol, client)
            .withProxyEventListener(proxy("first", calls))
            .addInstrumentationListener("context", recording("context", calls));
        startRegistration(builder, "first", protocol);
        assertSame(builder, builder.withProxyEventListener(proxy("second", calls)));
        builder.addInstrumentationListener("context", recording("ignored", calls));
        startRegistration(builder, "replaced", protocol);
        builder.withProxyEventListener(null);
        startRegistration(builder, "reset", protocol);

        assertForwarded(client, "replaced", calls, List.of("context", "second", "close context"), List.of("second"));
        assertForwarded(client, "reset", calls, List.of("context", "close context"), List.of());
        assertForwarded(client, "first", calls, List.of("context", "first", "close context"), List.of("first"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"cranker_1.0", "cranker_3.0"})
    void nullListenersFailWithoutReservingKeysOrChangingExistingRegistrations(String protocol) throws Exception {
        HttpClient client = startRegistrationFixture(protocol);
        List<String> calls = new CopyOnWriteArrayList<>();
        CrankerConnectorBuilder builder = registrationBuilder(protocol, client);
        assertThrows(NullPointerException.class, () -> builder.addInstrumentationListener(null, null));
        assertThrows(NullPointerException.class, () -> builder.addInstrumentationListener("key", null));
        builder.addInstrumentationListener("key", recording("valid", calls));
        assertThrows(NullPointerException.class, () -> builder.addInstrumentationListener("key", null));
        startRegistration(builder, "valid", protocol);

        assertForwarded(client, "valid", calls, List.of("valid", "close valid"), List.of());
    }

    @ParameterizedTest
    @CsvSource({
        "cranker_1.0, false, unchanged", "cranker_1.0, true, unchanged",
        "cranker_1.0, false, pending", "cranker_1.0, true, pending",
        "cranker_1.0, false, replacement", "cranker_1.0, true, replacement",
        "cranker_3.0, false, unchanged", "cranker_3.0, true, unchanged",
        "cranker_3.0, false, pending", "cranker_3.0, true, pending",
        "cranker_3.0, false, replacement", "cranker_3.0, true, replacement"
    })
    void originalProxyContractSendsExactlyItsReturnedRequest(String protocol, boolean instrumented, String mode)
        throws Exception {
        HttpClient client = startRegistrationFixture(protocol);
        AtomicReference<HttpRequest> returned = new AtomicReference<>();
        AtomicReference<HttpRequest> sent = new AtomicReference<>();
        HttpClient dispatchClient = new DelegatingHttpClient(client) {
            @Override
            public <T> CompletableFuture<HttpResponse<T>> sendAsync(HttpRequest request, HttpResponse.BodyHandler<T> handler) {
                assertSame(returned.get(), request);
                assertFalse(request.headers().firstValue("x-pending").isPresent());
                sent.set(request);
                return super.sendAsync(request, handler);
            }
        };
        List<String> calls = new CopyOnWriteArrayList<>();
        CrankerConnectorBuilder builder = registrationBuilder(protocol, dispatchClient)
            .withProxyEventListener(new ProxyEventListener() {
                @Override
                public HttpRequest beforeProxyToTarget(HttpRequest request, HttpRequest.Builder builder) {
                    if ("pending".equals(mode)) builder.header("x-pending", "not-built");
                    HttpRequest result = "replacement".equals(mode)
                        ? HttpRequest.newBuilder(request.uri()).timeout(Duration.ofSeconds(5))
                            .version(HttpClient.Version.HTTP_1_1).expectContinue(true)
                            .POST(HttpRequest.BodyPublishers.ofString("replacement-body")).build()
                        : request;
                    returned.set(result);
                    return result;
                }
            });
        if (instrumented) builder.addInstrumentationListener("context", recording("context", calls));
        startRegistration(builder, "*", protocol);

        HttpResponse<String> response = client.send(HttpRequest.newBuilder(routerServer.uri().resolve("/test"))
            .timeout(Duration.ofSeconds(15)).POST(HttpRequest.BodyPublishers.ofString("original-body")).build(),
            HttpResponse.BodyHandlers.ofString());

        assertEquals(200, response.statusCode());
        assertEquals("replacement".equals(mode) ? "replacement-body" : "original-body", response.body());
        assertNotNull(sent.get());
        assertSame(returned.get(), sent.get());
        assertEventually(() -> new ArrayList<>(calls),
            is(instrumented ? List.of("context", "close context") : List.of()));
    }

    private HttpClient startRegistrationFixture(String protocol) {
        targetServer = httpServer().withInterface("127.0.0.1")
            .addHandler((request, response) -> {
                request.headers().getAll("x-proxy").forEach(value -> response.headers().add("x-proxy", value));
                response.write(request.readBodyAsString());
                return true;
            }).start();
        router = crankerRouter().withSupportedCrankerProtocols(List.of(protocol)).start();
        routerServer = httpServer().withInterface("127.0.0.1")
            .addHandler(router.createRegistrationHandler()).addHandler(router.createHttpHandler()).start();
        return HttpClient.newBuilder().executor(clientExecutor)
            .connectTimeout(Duration.ofSeconds(5)).followRedirects(HttpClient.Redirect.NEVER)
            .version(HttpClient.Version.HTTP_1_1).build();
    }

    private CrankerConnectorBuilder registrationBuilder(String protocol, HttpClient client) {
        return CrankerConnectorBuilder.connector()
            .withTarget(targetServer.uri()).withHttpClient(client)
            .withPreferredProtocols(List.of(protocol)).withSlidingWindowSize(1)
            .withRouterUris(RegistrationUriSuppliers.fixedUris(List.of(
                BaseEndToEndTest.registrationUri(routerServer.uri()))));
    }

    private void startRegistration(CrankerConnectorBuilder builder, String route, String protocol) {
        CrankerConnector connector = builder.withRoute(route).build();
        connectors.add(connector);
        connector.start();
        BaseEndToEndTest.waitForRegistration(route, connector.connectorId(), 1, router);
        assertEventually(() -> connector.routers().get(0).idleSockets().stream()
            .filter(socket -> protocol.equals(socket.version())).count(), is(1L));
    }

    private void assertForwarded(HttpClient client, String route, List<String> calls,
                                 List<String> expectedCalls, List<String> expectedProxyHeaders) throws Exception {
        String body = "body-" + route;
        HttpResponse<String> response = client.send(HttpRequest.newBuilder(routerServer.uri().resolve("/" + route + "/test"))
            .timeout(Duration.ofSeconds(15)).POST(HttpRequest.BodyPublishers.ofString(body)).build(),
            HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), response.body());
        assertEquals(body, response.body());
        assertEquals(expectedProxyHeaders, response.headers().allValues("x-proxy"));
        assertEventually(() -> new ArrayList<>(calls), is(expectedCalls));
        calls.clear();
    }

    private static ConnectorInstrumentationListener recording(String name, List<String> calls) {
        return new ConnectorInstrumentationListener() {
            @Override
            public Runnable beforeTargetDispatch(TargetDispatchInfo info) {
                calls.add(name);
                return () -> calls.add("close " + name);
            }
        };
    }

    private static ProxyEventListener proxy(String name, List<String> calls) {
        return new ProxyEventListener() {
            @Override
            public HttpRequest beforeProxyToTarget(HttpRequest request, HttpRequest.Builder builder) {
                calls.add(name);
                return builder.header("x-proxy", name).build();
            }
        };
    }

    private HttpRequest request(String id) {
        return HttpRequest.newBuilder(routerServer.uri().resolve("/test"))
            .timeout(Duration.ofSeconds(25)).header("x-id", id)
            .header("x-repeated", "one").header("x-repeated", "two")
            .header("x-general-context", "user-value")
            .POST(HttpRequest.BodyPublishers.ofString("body-" + id)).build();
    }
}
