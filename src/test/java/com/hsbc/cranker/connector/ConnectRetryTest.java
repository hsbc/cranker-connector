package com.hsbc.cranker.connector;


import com.hsbc.cranker.mucranker.CrankerRouter;
import io.muserver.Http2ConfigBuilder;
import io.muserver.MuServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hsbc.cranker.connector.BaseEndToEndTest.*;
import static com.hsbc.cranker.mucranker.CrankerRouterBuilder.crankerRouter;
import static io.muserver.MuServerBuilder.httpServer;
import static io.muserver.MuServerBuilder.httpsServer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static scaffolding.Action.swallowException;
import static scaffolding.AssertUtils.assertEventually;
import static scaffolding.StringUtils.randomAsciiStringOfLength;

public class ConnectRetryTest {

    protected final HttpClient testClient = HttpUtils.createHttpClientBuilder(true).build();
    protected CrankerRouter crankerRouter;
    private MuServer targetServer;
    private MuServer router;
    private CrankerConnector connector;

    @AfterEach
    public void after() {
        if (targetServer != null) swallowException(targetServer::stop);
        if (router != null) swallowException(router::stop);
        if (connector != null) swallowException(() -> connector.stop(10, TimeUnit.SECONDS));
    }

    @RepeatedTest(3)
    public void ifTheRouterStopsThenTheConnectorWillReconnectWhenItStartsAgain(RepetitionInfo repetitionInfo) throws Exception {

        this.targetServer = httpServer()
            .addHandler((request, response) -> {
                response.write(request.readBodyAsString());
                return true;
            })
            .start();

        this.crankerRouter = crankerRouter()
            .withSupportedCrankerProtocols(List.of("cranker_3.0", "cranker_1.0"))
            .start();
        this.router = httpsServer()
            .addHandler(crankerRouter.createRegistrationHandler())
            .addHandler(crankerRouter.createHttpHandler())
            .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(false))
            .start();

        this.connector = startConnectorAndWaitForRegistration(crankerRouter, "*", targetServer, preferredProtocols(repetitionInfo), 2, router);

        String body = randomAsciiStringOfLength(100);
        HttpResponse<String> resp = testClient.send(HttpRequest.newBuilder()
            .method("POST", HttpRequest.BodyPublishers.ofString(body))
            .uri(router.uri())
            .build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(body, resp.body());

        int originalPort = router.uri().getPort();
        router.stop();

        assertThrows(IOException.class, () -> testClient.send(HttpRequest.newBuilder().uri(router.uri()).build(), HttpResponse.BodyHandlers.ofString()));

        Thread.sleep(2000);

        assertThat(connector.routers().get(0).currentUnsuccessfulConnectionAttempts(), greaterThan(0));

        this.crankerRouter = crankerRouter()
            .withSupportedCrankerProtocols(List.of("cranker_3.0", "cranker_1.0"))
            .start();
        this.router = httpsServer()
            .withHttpsPort(originalPort)
            .addHandler(crankerRouter.createRegistrationHandler())
            .addHandler(crankerRouter.createHttpHandler())
            .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(false))
            .start();

        String newBody = randomAsciiStringOfLength(100);
        assertEventually(() -> {
            HttpResponse<String> newResp = testClient.send(HttpRequest.newBuilder()
                .method("POST", HttpRequest.BodyPublishers.ofString(newBody))
                .uri(router.uri())
                .build(), HttpResponse.BodyHandlers.ofString());
            return newResp.body();
        }, is(newBody));

        assertThat(connector.routers().get(0).currentUnsuccessfulConnectionAttempts(), is(0));

    }

    @RepeatedTest(3)
    void connectionErrorListenerIsTriggeredOnWssConnectionFailed(RepetitionInfo repetitionInfo) throws InterruptedException, IOException {

        AtomicInteger exceptionCount = new AtomicInteger(0);
        int slidingWindow = 2;
        this.targetServer = httpServer()
            .addHandler((request, response) -> {
                final String text = request.readBodyAsString();
                response.write(text);
                return true;
            })
            .start();

        this.crankerRouter = crankerRouter()
            .withSupportedCrankerProtocols(List.of("cranker_3.0", "cranker_1.0"))
            .start();

        this.router = httpsServer()
            .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(false))
            .addHandler(crankerRouter.createRegistrationHandler())
            .addHandler(crankerRouter.createHttpHandler())
            .start();

        this.connector = CrankerConnectorBuilder.connector()
            .withPreferredProtocols(preferredProtocols(repetitionInfo))
            .withHttpClient(CrankerConnectorBuilder.createHttpClient(true).build())
            .withTarget(targetServer.uri())
            .withRoute("*")
            .withRouterUris(RegistrationUriSuppliers.fixedUris(
                URI.create("ws" + router.uri().toString().substring(4))))
            .withSlidingWindowSize(slidingWindow)
            .withRouterRegistrationListener(new RouterEventListener() {
                @Override
                public void onSocketConnectionError(RouterRegistration router, Throwable exception) {
                    exceptionCount.incrementAndGet();
                }
            })
            .start();

        waitForRegistration("*", connector.connectorId(), slidingWindow, crankerRouter);

        assertThat(exceptionCount.get(), equalTo(0));

        verifyHttpRequestWorking();

        int originalPort = router.uri().getPort();
        crankerRouter.stop();
        router.stop();

        assertThrows(IOException.class, () -> testClient.send(HttpRequest.newBuilder().uri(router.uri()).build(), HttpResponse.BodyHandlers.ofString()));

        assertEventually(exceptionCount::get, greaterThan(0));

        this.crankerRouter = crankerRouter()
            .withSupportedCrankerProtocols(List.of("cranker_3.0", "cranker_1.0"))
            .start();
        this.router = httpsServer()
            .withHttpsPort(originalPort)
            .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(false))
            .addHandler(crankerRouter.createRegistrationHandler())
            .addHandler(crankerRouter.createHttpHandler())
            .start();

        waitForRegistration("*", connector.connectorId(), slidingWindow, crankerRouter);

        verifyHttpRequestWorking();
    }

    private void verifyHttpRequestWorking() throws IOException, InterruptedException {
        String body = "hello";
        HttpResponse<String> resp = testClient.send(HttpRequest.newBuilder()
            .method("POST", HttpRequest.BodyPublishers.ofString(body))
            .uri(router.uri())
            .build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode());
        assertEquals(body, resp.body());
    }
}
