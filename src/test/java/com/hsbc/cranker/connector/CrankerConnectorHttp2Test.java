package com.hsbc.cranker.connector;

import com.hsbc.cranker.mucranker.CrankerRouter;
import io.muserver.Http2ConfigBuilder;
import io.muserver.Method;
import io.muserver.MuServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hsbc.cranker.connector.BaseEndToEndTest.preferredProtocols;
import static com.hsbc.cranker.connector.BaseEndToEndTest.startConnectorAndWaitForRegistration;
import static com.hsbc.cranker.mucranker.CrankerRouterBuilder.crankerRouter;
import static io.muserver.MuServerBuilder.httpsServer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static scaffolding.Action.swallowException;

public class CrankerConnectorHttp2Test {

    private final HttpClient http2Client = HttpUtils.createHttpClientBuilder(true)
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
    void canWorkWithHttp2MicroServiceAndHttp1Cranker(RepetitionInfo repetitionInfo) throws IOException, InterruptedException {

        System.getProperties().setProperty("jdk.internal.httpclient.disableHostnameVerification", Boolean.TRUE.toString());

        this.targetServer = httpsServer()
            .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(true))
            .addHandler(Method.GET, "/test", (request, response, pathParams) -> response.write("hello world"))
            .start();

        this.crankerRouter = crankerRouter()
            .withSupportedCrankerProtocols(List.of("cranker_3.0", "cranker_1.0"))
            .start();

        this.routerServer = httpsServer()
            .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(false))
            .addHandler(crankerRouter.createRegistrationHandler())
            .addHandler(crankerRouter.createHttpHandler())
            .start();

        this.connector = startConnectorAndWaitForRegistration(crankerRouter, "*", targetServer, preferredProtocols(repetitionInfo),2, this.routerServer);

        HttpResponse<String> response = http2Client.send(HttpRequest.newBuilder()
            .uri(this.routerServer.uri().resolve("/test"))
            .build(), HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode(), is(200));
        assertThat(response.headers().firstValue(":status").isPresent(), is(false));
        assertThat(response.body(), is("hello world"));
    }
}
