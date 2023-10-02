package com.hsbc.cranker.connector;

import com.hsbc.cranker.mucranker.CrankerRouter;
import io.muserver.ContentTypes;
import io.muserver.Http2ConfigBuilder;
import io.muserver.Method;
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

import static com.hsbc.cranker.connector.BaseEndToEndTest.preferredProtocols;
import static com.hsbc.cranker.connector.BaseEndToEndTest.startConnectorAndWaitForRegistration;
import static com.hsbc.cranker.mucranker.CrankerRouterBuilder.crankerRouter;
import static io.muserver.MuServerBuilder.httpsServer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static scaffolding.Action.swallowException;

class CrankerConnectorBuilderTest {

    private final HttpClient http2Client = HttpUtils.createHttpClientBuilder(true)
        .version(HttpClient.Version.HTTP_2)
        .build();

    private final HttpClient http1Client = HttpUtils.createHttpClientBuilder(true)
        .version(HttpClient.Version.HTTP_1_1)
        .build();

    private CrankerRouter crankerRouter;
    private MuServer targetServer;
    private MuServer crankerServer;
    private CrankerConnector connector;

    @AfterEach
    public void after() {
        if (connector != null) swallowException(() -> connector.stop(10, TimeUnit.SECONDS));
        if (targetServer != null) swallowException(targetServer::stop);
        if (crankerServer != null) swallowException(crankerServer::stop);
        if (crankerRouter != null) swallowException(crankerRouter::stop);
    }

    @RepeatedTest(3)
    void allowSlash() {
        CrankerConnectorBuilder.connector().withTarget(URI.create("http://localhost:1234")).withRoute("hello").build();
        CrankerConnectorBuilder.connector().withTarget(URI.create("http://localhost:1234")).withRoute("hello_123").build();
        CrankerConnectorBuilder.connector().withTarget(URI.create("http://localhost:1234")).withRoute("hello-123").build();
        CrankerConnectorBuilder.connector().withTarget(URI.create("http://localhost:1234")).withRoute("hello/123").build();
    }

    @RepeatedTest(3)
    void testMaxHeadersSize_normal(RepetitionInfo repetitionInfo) throws IOException, InterruptedException {

        setupServerForMaxHeaders(40000, preferredProtocols(repetitionInfo));

        final String bigHeader = "b".repeat(18000);

        HttpResponse<String> resp1 = http1Client.send(HttpRequest.newBuilder()
            .uri(crankerServer.uri().resolve("/test"))
            .header("big-header", bigHeader)
            .build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp1.statusCode());
        assertEquals(bigHeader, resp1.headers().firstValue("big-header").orElse("not exist"));

        HttpResponse<String> resp2 = http2Client.send(HttpRequest.newBuilder()
            .uri(crankerServer.uri().resolve("/test"))
            .header("big-header", bigHeader)
            .build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp2.statusCode());
        assertEquals(bigHeader, resp2.headers().firstValue("big-header").orElse("not exist"));
    }

    @RepeatedTest(3)
    void testMaxHeadersSize_exception(RepetitionInfo repetitionInfo) throws IOException, InterruptedException {

        setupServerForMaxHeaders(40000,  preferredProtocols(repetitionInfo));

        final String bigHeader = "b".repeat(58000); // test header size from 2000 to 20000

        HttpResponse<String> resp1 = http1Client.send(HttpRequest.newBuilder()
            .uri(crankerServer.uri().resolve("/test"))
            .header("big-header", bigHeader)
            .build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(431, resp1.statusCode());

        HttpResponse<String> resp2 = http2Client.send(HttpRequest.newBuilder()
            .uri(crankerServer.uri().resolve("/test"))
            .header("big-header", bigHeader)
            .build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(431, resp2.statusCode());
    }

    private void setupServerForMaxHeaders(int maxHeadersSize, List<String> preferredProtocols) {
        this.targetServer = httpsServer()
            .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(true))
            .withMaxHeadersSize(maxHeadersSize)
            .addHandler(Method.GET, "/test", (request, response, pathParams) -> {
                response.contentType(ContentTypes.TEXT_PLAIN_UTF8);
                response.headers().set("big-header", request.headers().get("big-header", "nothing"));
            })
            .start();

        this.crankerRouter = crankerRouter()
            .withSupportedCrankerProtocols(List.of("cranker_3.0", "cranker_1.0"))
            .start();

        this.crankerServer = httpsServer()
            .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(false))
            .withMaxHeadersSize(maxHeadersSize)
            .addHandler(crankerRouter.createRegistrationHandler())
            .addHandler(crankerRouter.createHttpHandler())
            .start();

        this.connector = startConnectorAndWaitForRegistration(crankerRouter, "*", targetServer, preferredProtocols, 2, this.crankerServer);
    }
}
