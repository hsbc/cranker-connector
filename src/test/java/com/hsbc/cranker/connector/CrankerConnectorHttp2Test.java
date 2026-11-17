package com.hsbc.cranker.connector;

import com.hsbc.cranker.mucranker.CrankerRouter;
import com.hsbc.cranker.mucranker.CrankerRouterBuilder;
import io.muserver.Http2ConfigBuilder;
import io.muserver.MuServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static io.muserver.MuServerBuilder.httpsServer;
import static java.net.http.HttpClient.Version.HTTP_1_1;
import static java.net.http.HttpClient.Version.HTTP_2;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static scaffolding.Action.swallowException;
import static scaffolding.AssertUtils.assertEventually;

public class CrankerConnectorHttp2Test {

    private final HttpClient ingressClient = HttpUtils.createHttpClientBuilder(true)
        .connectTimeout(Duration.ofSeconds(10)).build();
    private final AtomicReference<HttpRequest> requestToTarget = new AtomicReference<>();
    private final AtomicReference<String> targetProtocol = new AtomicReference<>();
    private final AtomicReference<String> targetHost = new AtomicReference<>();
    private CrankerRouter crankerRouter;
    private MuServer targetServer;
    private MuServer routerServer;
    private CrankerConnector connector;

    @AfterEach
    public void after() {
        if (connector != null) swallowException(() -> connector.stop(10, TimeUnit.SECONDS));
        if (targetServer != null) swallowException(targetServer::stop);
        if (routerServer != null) swallowException(routerServer::stop);
        if (crankerRouter != null) swallowException(crankerRouter::stop);
    }

    static Stream<List<String>> tunnelProtocols() {
        return Stream.of(List.of("cranker_1.0"), List.of("cranker_3.0"), List.of("cranker_3.0", "cranker_1.0"));
    }

    static Stream<Arguments> hostForwardingCases() {
        return tunnelProtocols().flatMap(protocols -> {
            List<Arguments> cases = new ArrayList<>();
            for (boolean proxyHost : List.of(false, true)) {
                for (HttpClient.Version ingress : List.of(HTTP_1_1, HTTP_2)) {
                    for (HttpClient.Version target : List.of(HTTP_1_1, HTTP_2)) {
                        cases.add(Arguments.of(protocols, proxyHost, ingress, target));
                    }
                }
            }
            return cases.stream();
        });
    }

    @ParameterizedTest(name = "{0}, proxyHost={1}, ingress={2}, target client={3}")
    @MethodSource("hostForwardingCases")
    void hostForwardingDeterminesTargetProtocol(List<String> protocols, boolean proxyHost,
                                               HttpClient.Version ingress, HttpClient.Version target) throws Exception {
        start(protocols, proxyHost, target, true, (request, builder) -> request);
        // H2 ingress needs a valid authority/Host pair; its router authority still differs from the target.
        String originalHost = ingress == HTTP_2 ? routerServer.uri().getAuthority() : "public.example";
        assertNotEquals(originalHost, targetServer.uri().getAuthority());
        assertProxyResponse(ingress, originalHost, proxyHost ? originalHost : null, proxyHost ? HTTP_1_1 : target);
    }

    @ParameterizedTest
    @MethodSource("tunnelProtocols")
    void noHostAllowsHttp1Fallback(List<String> protocols) throws Exception {
        start(protocols, false, HTTP_2, false, (request, builder) -> request);
        assertProxyResponse(HTTP_1_1, "public.example", null, HTTP_1_1);
    }

    @ParameterizedTest
    @MethodSource("tunnelProtocols")
    void matchingHostAlsoForcesHttp1(List<String> protocols) throws Exception {
        start(protocols, true, HTTP_2, true, (request, builder) -> request);
        String host = targetServer.uri().getAuthority();
        assertProxyResponse(HTTP_1_1, host, host, HTTP_1_1);
    }

    @ParameterizedTest
    @MethodSource("tunnelProtocols")
    void hostAddedByListenerForcesHttp1EvenWithExplicitHttp2Preference(List<String> protocols) throws Exception {
        start(protocols, false, HTTP_2, true,
            (request, builder) -> builder.header("HoSt", "listener.example").version(HTTP_2).build());
        assertProxyResponse(HTTP_1_1, "public.example", "listener.example", HTTP_1_1);
    }

    @ParameterizedTest
    @MethodSource("tunnelProtocols")
    void hostRemovedByListenerAllowsHttp2(List<String> protocols) throws Exception {
        start(protocols, true, HTTP_2, true, (request, builder) -> {
            // Java 11 has no removeHeader API: return a replacement request without Host.
            HttpRequest.Builder replacement = HttpRequest.newBuilder(request.uri())
                .method(request.method(), request.bodyPublisher().orElse(HttpRequest.BodyPublishers.noBody()))
                .version(HTTP_2);
            request.headers().map().forEach((name, values) -> {
                if (!name.equalsIgnoreCase("host")) values.forEach(value -> replacement.header(name, value));
            });
            return replacement.build();
        });
        assertProxyResponse(HTTP_1_1, "public.example", null, HTTP_2);
    }

    private void start(List<String> protocols, boolean proxyHost, HttpClient.Version targetVersion,
                       boolean targetSupportsHttp2, BiFunction<HttpRequest, HttpRequest.Builder, HttpRequest> customize) {
        assumeFalse(HttpUtils.DISALLOWED_REQUEST_HEADERS.contains("host"), "This JDK cannot set Host");
        targetServer = httpsServer()
            .withInterface("127.0.0.1")
            .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(targetSupportsHttp2))
            .addHandler((request, response) -> {
                targetProtocol.set(request.connection().protocol().toString());
                targetHost.set(request.headers().get("host"));
                response.write(request.method().name() + ": " + request.readBodyAsString());
                return true;
            })
            .start();
        crankerRouter = CrankerRouterBuilder.crankerRouter()
            .withSupportedCrankerProtocols(List.of("cranker_3.0", "cranker_1.0"))
            .proxyHostHeader(proxyHost)
            .start();
        routerServer = httpsServer()
            .withInterface("127.0.0.1")
            .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(true))
            .addHandler(crankerRouter.createRegistrationHandler())
            .addHandler(crankerRouter.createHttpHandler())
            .start();
        connector = CrankerConnectorBuilder.connector()
            .withPreferredProtocols(protocols)
            .withHttpClient(CrankerConnectorBuilder.createHttpClient(true)
                .connectTimeout(Duration.ofSeconds(10))
                .version(targetVersion)
                .build())
            .withProxyEventListener(new ProxyEventListener() {
                @Override
                public HttpRequest beforeProxyToTarget(HttpRequest request, HttpRequest.Builder requestBuilder) {
                    HttpRequest customized = customize.apply(request, requestBuilder);
                    requestToTarget.set(customized);
                    return customized;
                }
            })
            .withRouterUris(RegistrationUriSuppliers.fixedUris(BaseEndToEndTest.registrationUri(routerServer.uri())))
            .withRoute("*")
            .withTarget(targetServer.uri())
            .start();
        BaseEndToEndTest.waitForRegistration("*", connector.connectorId(), 2, crankerRouter);
        assertEventually(() -> connector.routers().stream().flatMap(router -> router.idleSockets().stream())
            .anyMatch(socket -> protocols.get(0).equals(socket.version())), is(true));
    }

    private void assertProxyResponse(HttpClient.Version ingress, String originalHost, String explicitTargetHost,
                                     HttpClient.Version expectedTargetVersion) throws Exception {
        HttpResponse<String> response = ingressClient.send(HttpRequest.newBuilder(routerServer.uri().resolve("/test"))
            .version(ingress)
            .header("host", originalHost)
            .timeout(Duration.ofSeconds(10))
            .POST(HttpRequest.BodyPublishers.ofString("request body"))
            .build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), response.body());
        assertEquals(ingress, response.version());
        assertEquals("POST: request body", response.body());
        if (ingress == HTTP_1_1) assertFalse(response.headers().firstValue(":status").isPresent());
        assertNotNull(requestToTarget.get());
        assertEquals(explicitTargetHost, requestToTarget.get().headers().firstValue("host").orElse(null));
        assertEquals(targetServer.uri().getAuthority(), requestToTarget.get().uri().getAuthority());
        // Mu's H2 Host view comes from :authority, not necessarily an explicit wire header.
        assertEquals(explicitTargetHost == null ? targetServer.uri().getAuthority() : explicitTargetHost, targetHost.get());
        assertEquals(expectedTargetVersion == HTTP_2 ? "HTTP/2" : "HTTP/1.1", targetProtocol.get());
    }
}
