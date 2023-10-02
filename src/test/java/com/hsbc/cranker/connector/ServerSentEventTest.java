package com.hsbc.cranker.connector;

import io.muserver.Http2ConfigBuilder;
import io.muserver.Method;
import io.muserver.MuServer;
import io.muserver.SsePublisher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;
import org.junit.jupiter.api.Test;
import scaffolding.SseTestClient;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hsbc.cranker.mucranker.CrankerRouterBuilder.crankerRouter;
import static io.muserver.MuServerBuilder.httpServer;
import static io.muserver.MuServerBuilder.httpsServer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static scaffolding.Action.swallowException;

public class ServerSentEventTest extends BaseEndToEndTest {

    private SseTestClient client;
    private MuServer targetServer;
    private MuServer router;
    private CrankerConnector connector;

    @AfterEach
    public void after() {
        if (client != null) swallowException(client::stop);
        if (targetServer != null) swallowException(targetServer::stop);
        if (router != null) swallowException(router::stop);
        if (connector != null) swallowException(() -> connector.stop(10, TimeUnit.SECONDS));
    }

   @RepeatedTest(3)
    public void MuServer_NormalSseTest(RepetitionInfo repetitionInfo) throws Exception {

        this.targetServer = httpServer()
            .addHandler(Method.GET, "/sse/counter", (request, response, pathParams) -> {
                SsePublisher publisher = SsePublisher.start(request, response);
                publisher.send("Number 0");
                publisher.send("Number 1");
                publisher.send("Number 2");
                publisher.close();
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

       this.connector = startConnectorAndWaitForRegistration(crankerRouter, "*", targetServer,
           preferredProtocols(repetitionInfo),2, router);

        this.client = SseTestClient.startSse(router.uri().resolve("/sse/counter"));
        this.client.waitUntilClose(5, TimeUnit.SECONDS);

        assertThat(this.client.getMessages(), equalTo(Arrays.asList(
            "onOpen:",
            "onEvent: id=null, type=null, data=Number 0",
            "onEvent: id=null, type=null, data=Number 1",
            "onEvent: id=null, type=null, data=Number 2",
            "onClosed:"
        )));
    }

   @Test
    public void MuServer_TargetServerDownInMiddleTest_ClientTalkToTargetServer() throws Exception {

        this.targetServer = httpServer()
            .addHandler(Method.GET, "/sse/counter", (request, response, pathParams) -> {
                SsePublisher publisher = SsePublisher.start(request, response);
                publisher.send("Number 0");
                publisher.send("Number 1");
                publisher.send("Number 2");
                targetServer.stop();
            })
            .start();

        this.client = SseTestClient.startSse(targetServer.uri().resolve("/sse/counter"));
        this.client.waitUntilError(5, TimeUnit.SECONDS);

        assertThat(this.client.getMessages(), equalTo(Arrays.asList(
            "onOpen:",
            "onEvent: id=null, type=null, data=Number 0",
            "onEvent: id=null, type=null, data=Number 1",
            "onEvent: id=null, type=null, data=Number 2",
            "onFailure: message=null"
        )));
    }

   @RepeatedTest(3)
    public void MuServer_TargetServerDownInMiddleTest_ClientTalkToRouter(RepetitionInfo repetitionInfo) throws Exception {

        this.targetServer = httpServer()
            .addHandler(Method.GET, "/sse/counter", (request, response, pathParams) -> {
                SsePublisher publisher = SsePublisher.start(request, response);
                publisher.send("Number 0");
                publisher.send("Number 1");
                publisher.send("Number 2");
                targetServer.stop();
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

        this.connector = startConnectorAndWaitForRegistration(crankerRouter, "*", targetServer,
            preferredProtocols(repetitionInfo), 2, router);

        this.client = SseTestClient.startSse(router.uri().resolve("/sse/counter"));
        this.client.waitUntilError(100, TimeUnit.SECONDS);

        assertThat(this.client.getMessages(), contains(
            equalTo("onOpen:"),
            equalTo("onEvent: id=null, type=null, data=Number 0"),
            equalTo("onEvent: id=null, type=null, data=Number 1"),
            equalTo("onEvent: id=null, type=null, data=Number 2"),
            startsWith("onFailure: message=")
        ));
    }

}
