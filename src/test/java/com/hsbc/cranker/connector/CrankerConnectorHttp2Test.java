package com.hsbc.cranker.connector;

import io.muserver.Http2ConfigBuilder;
import io.muserver.Method;
import io.muserver.MuServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.TimeUnit;

import static io.muserver.MuServerBuilder.httpsServer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static scaffolding.Action.swallowException;
import static scaffolding.testrouter.CrankerRouterBuilder.crankerRouter;

public class CrankerConnectorHttp2Test extends BaseEndToEndTest{

    protected final HttpClient http2Client = HttpUtils.createHttpClientBuilder(true)
        .version(HttpClient.Version.HTTP_2)
        .build();
    private MuServer targetServer;
    private MuServer router;
    private CrankerConnector connector;

    @AfterEach
    public void after() {
        if (connector != null) swallowException(() -> connector.stop().get(10, TimeUnit.SECONDS));
        if (targetServer != null) swallowException(targetServer::stop);
        if (router != null) swallowException(router::stop);
    }

    @Test
    void canWorkWithHttp2MicroServiceAndHttp1Cranker() throws IOException, InterruptedException {

        System.getProperties().setProperty("jdk.internal.httpclient.disableHostnameVerification", Boolean.TRUE.toString());

        this.targetServer = httpsServer()
            .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(true))
            .addHandler(Method.GET, "/test", (request, response, pathParams) -> {
                response.write("hello world");
            })
            .start();

        this.crankerRouter = crankerRouter()
            .withConnectorAcquireAttempts(4, 100)
            .start();

        this.router = httpsServer()
            .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(false))
            .addHandler(crankerRouter.createRegistrationHandler())
            .addHandler(crankerRouter.createHttpHandler())
            .start();

        this.connector = startConnectorAndWaitForRegistration(crankerRouter, "*", targetServer, 2, router);

        HttpResponse<String> response = http2Client.send(HttpRequest.newBuilder()
            .uri(router.uri().resolve("/test"))
            .build(), HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode(), is(200));
        assertThat(response.headers().firstValue(":status").isPresent(), is(false));
        assertThat(response.body(), is("hello world"));
    }
}
