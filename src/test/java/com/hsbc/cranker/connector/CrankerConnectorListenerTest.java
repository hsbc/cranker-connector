package com.hsbc.cranker.connector;


import com.hsbc.cranker.mucranker.CrankerRouter;
import io.muserver.Http2ConfigBuilder;
import io.muserver.Method;
import io.muserver.MuServer;
import io.muserver.RouteHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.hsbc.cranker.connector.BaseEndToEndTest.preferredProtocols;
import static com.hsbc.cranker.mucranker.CrankerRouterBuilder.crankerRouter;
import static io.muserver.MuServerBuilder.httpsServer;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static scaffolding.Action.swallowException;

public class CrankerConnectorListenerTest {

    private final HttpClient httpClient = HttpUtils.createHttpClientBuilder(true)
        .version(HttpClient.Version.HTTP_2)
        .build();
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

    @RepeatedTest(3)
    void testProxyEventListenerInvoked(RepetitionInfo repetitionInfo) throws Exception {

        this.crankerRouter = crankerRouter()
            .withSupportedCrankerProtocols(List.of("cranker_3.0", "cranker_1.0"))
            .start();

        this.routerServer = httpsServer()
            .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(false))
            .addHandler(crankerRouter.createRegistrationHandler())
            .addHandler(crankerRouter.createHttpHandler())
            .start();

        final RouteHandler handler = (request, response, pathParams) -> {
            StringBuilder bodyBuilder = new StringBuilder();
            for (Map.Entry<String, String> header : request.headers()) {
                bodyBuilder.append(header.getKey()).append(":").append(header.getValue()).append("\n");
            }
            bodyBuilder.append("\n");
            bodyBuilder.append(request.readBodyAsString());
            response.status(200);
            response.write(bodyBuilder.toString());
        };

        this.targetServer = httpsServer()
            .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(true))
            .addHandler(Method.GET, "/test", handler)
            .addHandler(Method.POST, "/test", handler)
            .start();

        this.connector = CrankerConnectorBuilder.connector()
            .withPreferredProtocols(preferredProtocols(repetitionInfo))
            .withHttpClient(CrankerConnectorBuilder.createHttpClient(true).build())
            .withTarget(targetServer.uri())
            .withRoute("*")
            .withRouterUris(RegistrationUriSuppliers.fixedUris(Stream.of(new MuServer[]{this.routerServer})
                .map(s -> BaseEndToEndTest.registrationUri(s.uri()))
                .collect(toList())))
            .withSlidingWindowSize(2)
            .withProxyEventListener(new ProxyEventListener() {
                @Override
                public HttpRequest beforeProxyToTarget(HttpRequest request, HttpRequest.Builder requestBuilder) {
                    // add extra header
                    final String clientHeader = request.headers().firstValue("x-client-header").orElseGet(() -> "");
                    requestBuilder.header("x-connector-header", "connector-value_" + clientHeader);
                    return requestBuilder.build();
                }
            })
            .start();

        BaseEndToEndTest.waitForRegistration("*", connector.connectorId(), 2, crankerRouter);

        // GET
        HttpResponse<String> response = httpClient.send(HttpRequest.newBuilder()
            .header("x-client-header", "client-value")
            .uri(this.routerServer.uri().resolve("/test"))
            .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode(), is(200));
        final String body = response.body();
        assertThat(body, containsString("x-client-header:client-value\n"));
        assertThat(body, containsString("x-connector-header:connector-value_client-value\n"));

        // POST
        HttpResponse<String> response_2 = httpClient.send(HttpRequest.newBuilder()
            .header("x-client-header", "client-value")
            .uri(this.routerServer.uri().resolve("/test"))
            .method("POST", HttpRequest.BodyPublishers.ofString("this is request body string"))
            .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response_2.statusCode(), is(200));
        final String body_2 = response_2.body();
        assertThat(body_2, containsString("x-client-header:client-value\n"));
        assertThat(body_2, containsString("x-connector-header:connector-value_client-value\n"));
        assertThat(body_2, containsString("this is request body string"));

    }

    @RepeatedTest(3)
    void testRegistrationEventListener_supplyingAuthHeader(RepetitionInfo repetitionInfo) throws Exception {

        final String authHeader = "authHeader";
        final String authToken = "authToken";

        this.crankerRouter = crankerRouter()
            .withSupportedCrankerProtocols(List.of("cranker_3.0", "cranker_1.0"))
            .start();

        // router server with auth header checking
        this.routerServer = httpsServer()
            .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(false))
            .addHandler(((muRequest, muResponse) -> {
                if (!muRequest.uri().getPath().startsWith("/register")) {
                    return false;
                }
                if (muRequest.headers().contains(authHeader) && muRequest.headers().get(authHeader).equals(authToken)) {
                    return false;
                }

                muResponse.status(400);
                muResponse.write("Bad Request: missing auth header");
                return true;
            }))
            .addHandler(crankerRouter.createRegistrationHandler())
            .addHandler(crankerRouter.createHttpHandler())
            .start();

        this.targetServer = httpsServer()
            .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(true))
            .addHandler((request, response) -> {
                response.write("good");
                return true;
            })
            .start();

        CrankerConnector badConnector = null;
        try {

            // bad connector WITHOUT auth header
            badConnector = CrankerConnectorBuilder.connector()
                .withPreferredProtocols(preferredProtocols(repetitionInfo))
                .withHttpClient(CrankerConnectorBuilder.createHttpClient(true).build())
                .withTarget(targetServer.uri())
                .withRoute("bad")
                .withRouterUris(RegistrationUriSuppliers.fixedUris(Stream.of(new MuServer[]{this.routerServer})
                    .map(s -> BaseEndToEndTest.registrationUri(s.uri()))
                    .collect(toList())))
                .withSlidingWindowSize(2)
                .start();

            // good connector with auth header provided
            this.connector = CrankerConnectorBuilder.connector()
                .withPreferredProtocols(preferredProtocols(repetitionInfo))
                .withHttpClient(CrankerConnectorBuilder.createHttpClient(true).build())
                .withTarget(targetServer.uri())
                .withRoute("good")
                .withRouterUris(RegistrationUriSuppliers.fixedUris(Stream.of(new MuServer[]{this.routerServer})
                    .map(s -> BaseEndToEndTest.registrationUri(s.uri()))
                    .collect(toList())))
                .withRegistrationEventListener(new RegistrationEventListener() {
                    @Override
                    public void beforeRegisterToRouter(WebSocket.Builder builder) {
                        builder.header(authHeader, authToken);
                    }
                })
                .withSlidingWindowSize(2)
                .start();

            BaseEndToEndTest.waitForRegistration("good", connector.connectorId(), 2, crankerRouter);

            assertFalse(crankerRouter.collectInfo().services().stream().anyMatch(service -> service.route().equals("bad")));
            assertThat(httpClient.send(HttpRequest.newBuilder().uri(this.routerServer.uri().resolve("/bad"))
                .build(), HttpResponse.BodyHandlers.ofString()).statusCode(), is(404));

            assertTrue(crankerRouter.collectInfo().services().stream().anyMatch(service -> service.route().equals("good")));
            assertThat(httpClient.send(HttpRequest.newBuilder().uri(this.routerServer.uri().resolve("/good"))
                .build(), HttpResponse.BodyHandlers.ofString()).statusCode(), is(200));
        } finally {
            if (badConnector != null) {
                badConnector.stop(10, TimeUnit.SECONDS);
            }
        }
    }

}
