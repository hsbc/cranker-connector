package com.hsbc.cranker.connector;


import com.hsbc.cranker.mucranker.CrankerRouter;
import io.muserver.*;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scaffolding.AssertUtils;

import java.io.*;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hsbc.cranker.connector.BaseEndToEndTest.*;
import static com.hsbc.cranker.mucranker.CrankerRouterBuilder.crankerRouter;
import static io.muserver.MuServerBuilder.httpServer;
import static io.muserver.MuServerBuilder.httpsServer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;
import static scaffolding.Action.swallowException;
import static scaffolding.AssertUtils.assertEventually;

public class CrankerConnectorStopTest {

    private static final Logger log = LoggerFactory.getLogger(CrankerConnectorStopTest.class);

    private final HttpClient httpClient = HttpUtils.createHttpClientBuilder(true)
        .version(HttpClient.Version.HTTP_2)
        .build();
    private CrankerRouter crankerRouter;
    private MuServer targetServer;
    private MuServer routerServer;
    private CrankerConnector connector;

    private final AtomicInteger counter = new AtomicInteger(0);
    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(10, r -> new Thread(r, "test-pool" + counter.incrementAndGet()));

    @AfterEach
    public void after() {
        if (connector != null) connector.stop(10, TimeUnit.SECONDS);
        if (targetServer != null) swallowException(targetServer::stop);
        if (routerServer != null) swallowException(routerServer::stop);
        if (crankerRouter != null) swallowException(crankerRouter::stop);
        swallowException(executorService::shutdownNow);
    }

    @Test
    @Timeout(30)
    void theHttpClientCanBeStopped() {

        this.targetServer = httpsServer().start();

        this.crankerRouter = crankerRouter()
            .withSupportedCrankerProtocols(List.of("cranker_3.0", "cranker_1.0"))
            .start();

        this.routerServer = httpsServer()
            .addHandler(crankerRouter.createRegistrationHandler())
            .addHandler(crankerRouter.createHttpHandler())
            .start();

        this.connector = startConnectorAndWaitForRegistration(crankerRouter, "*", targetServer,
            List.of("cranker_1.0"),2, this.routerServer);

        assertThat(this.connector.stop(10, TimeUnit.SECONDS), is(true));
        assertDoesNotThrow(() -> {
            Object client = connector.httpClient();
            if (client instanceof AutoCloseable) {
                assertDoesNotThrow(() -> ((AutoCloseable)client).close());
            }
        });
        this.connector = null;
    }

    @RepeatedTest(3)
    void requestOnTheFlyShouldCompleteSuccessfullyWithinTimeout(RepetitionInfo repetitionInfo) {

        AtomicInteger serverCounter = new AtomicInteger(0);
        AtomicInteger clientCounter = new AtomicInteger(0);

        this.targetServer = httpsServer()
            .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(true))
            .addHandler(Method.GET, "/test", (request, response, pathParams) -> {
                serverCounter.incrementAndGet();
                final AsyncHandle asyncHandle = request.handleAsync();
                executorService.schedule(() -> {
                    response.status(201);
                    asyncHandle.write(ByteBuffer.wrap("hello world".getBytes()));
                    asyncHandle.complete();
                }, 2, TimeUnit.SECONDS);
            })
            .start();

        this.crankerRouter = crankerRouter()
            .withSupportedCrankerProtocols(List.of("cranker_3.0", "cranker_1.0"))
            .start();

        this.routerServer = httpsServer()
            .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(false))
            .addHandler(crankerRouter.createRegistrationHandler())
            .addHandler(crankerRouter.createHttpHandler())
            .start();

        this.connector = startConnectorAndWaitForRegistration(crankerRouter, "*", targetServer,
            preferredProtocols(repetitionInfo),2, this.routerServer);

        int requestCount = 3;
        for (int i = 0; i < requestCount; i++) {
            executorService.submit(() -> swallowException(() -> {
                log.info("client: sending request");
                HttpResponse<String> response = httpClient.send(HttpRequest.newBuilder()
                    .uri(this.routerServer.uri().resolve("/test"))
                    .build(), HttpResponse.BodyHandlers.ofString());
                assertThat(response.statusCode(), is(201));
                assertThat(response.body(), is("hello world"));
                clientCounter.incrementAndGet();
                log.info("client: request completed");
            }));
        }

        assertEventually(serverCounter::get, equalTo(requestCount));
        assertEventually(clientCounter::get, lessThan(requestCount));

        assertThat(this.connector.stop(10, TimeUnit.SECONDS), is(true));
        this.targetServer.stop();

        assertEventually(clientCounter::get, equalTo(requestCount));
    }

    @RepeatedTest(3)
    void requestOnTheFlyShouldCompleteSuccessfullyWithinTimeout_LongQuery(RepetitionInfo repetitionInfo) {

        AtomicInteger serverCounter = new AtomicInteger(0);
        AtomicInteger clientCounter = new AtomicInteger(0);

        this.targetServer = httpsServer()
            .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(true))
            .addHandler(Method.GET, "/test", (request, response, pathParams) -> {
                serverCounter.incrementAndGet();
                response.contentType(ContentTypes.TEXT_PLAIN_UTF8);
                try (PrintWriter writer = response.writer()) {
                    for (int i = 0; i < 12; i++) {
                        writer.print(i + ",");
                        writer.flush();
                        Thread.sleep(100L);
                    }
                }
            })
            .start();

        this.crankerRouter = crankerRouter()
            .withSupportedCrankerProtocols(List.of("cranker_3.0", "cranker_1.0"))
            .start();

        this.routerServer = httpsServer()
            .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(false))
            .addHandler(crankerRouter.createRegistrationHandler())
            .addHandler(crankerRouter.createHttpHandler())
            .start();

        this.connector = startConnectorAndWaitForRegistration(crankerRouter, "*", targetServer,
            preferredProtocols(repetitionInfo),2, this.routerServer);

        executorService.submit(() -> {
            try {
                HttpResponse<String> response = httpClient.send(HttpRequest.newBuilder()
                    .uri(this.routerServer.uri().resolve("/test"))
                    .build(), HttpResponse.BodyHandlers.ofString());
                assertThat(response.statusCode(), is(200));
                assertThat(response.body(), is("0,1,2,3,4,5,6,7,8,9,10,11,"));
                clientCounter.incrementAndGet();
            } catch (Throwable throwable) {
                log.error("client error", throwable);
                fail("client error");
            }
        });

        assertEventually(serverCounter::get, equalTo(1));

        assertThat(this.connector.stop(15, TimeUnit.SECONDS), is(true));
        this.targetServer.stop();

        assertEventually(clientCounter::get, equalTo(1));
    }


    @RepeatedTest(3)
    void getTimeoutExceptionIfExceedTimeout(RepetitionInfo repetitionInfo) {

        AtomicInteger serverCounter = new AtomicInteger(0);
        AtomicInteger serverExceptionCounter = new AtomicInteger(0);
        AtomicInteger clientCounter = new AtomicInteger(0);

        this.targetServer = httpsServer()
            .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(true))
            .addHandler(Method.GET, "/test", (request, response, pathParams) -> {
                serverCounter.incrementAndGet();
                final AsyncHandle asyncHandle = request.handleAsync();
                executorService.schedule(() -> {
                    try {
                        response.status(201);
                        asyncHandle.write(ByteBuffer.wrap("hello world".getBytes()));
                        asyncHandle.complete();
                    } catch (Throwable throwable) {
                        serverExceptionCounter.incrementAndGet();
                    }
                }, 3, TimeUnit.SECONDS);
            })
            .start();

        this.crankerRouter = crankerRouter()
            .withSupportedCrankerProtocols(List.of("cranker_3.0", "cranker_1.0"))
            .start();

        this.routerServer = httpsServer()
            .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(false))
            .addHandler(crankerRouter.createRegistrationHandler())
            .addHandler(crankerRouter.createHttpHandler())
            .start();

        this.connector = startConnectorAndWaitForRegistration(crankerRouter, "*", targetServer,
            preferredProtocols(repetitionInfo),2, this.routerServer);

        int requestCount = 3;
        for (int i = 0; i < requestCount; i++) {
            executorService.submit(() -> swallowException(() -> {
                log.info("client: sending request");
                HttpResponse<String> response = httpClient.send(HttpRequest.newBuilder()
                    .uri(this.routerServer.uri().resolve("/test"))
                    .build(), HttpResponse.BodyHandlers.ofString());
                assertThat(response.statusCode(), is(201));
                assertThat(response.body(), is("hello world"));
                clientCounter.incrementAndGet();
                log.info("client: request completed");
            }));
        }

        assertEventually(serverCounter::get, equalTo(requestCount));
        assertEventually(clientCounter::get, lessThan(requestCount));


        assertThat(this.connector.stop(1, TimeUnit.SECONDS), is(false));
        this.targetServer.stop();
    }

    @RepeatedTest(3)
    public void throwIllegalStateExceptionWhenCallingStopBeforeCallingStart(RepetitionInfo repetitionInfo) {

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

        this.connector = CrankerConnectorBuilder.connector()
            .withPreferredProtocols(preferredProtocols(repetitionInfo))
            .withHttpClient(CrankerConnectorBuilder.createHttpClient(true).build())
            .withRouterUris(RegistrationUriSuppliers.fixedUris(URI.create("wss://localhost:1234")))
            .withRoute("*")
            .withTarget(URI.create("https://test-url"))
            .withComponentName("cranker-connector-unit-test")
            .build(); // not start


        assertFalse(connector.stop(1, TimeUnit.SECONDS));
    }


    @RepeatedTest(3)
    public void throwIllegalStateExceptionWhenCallingStopMultipleTime(RepetitionInfo repetitionInfo) {

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

        this.connector = CrankerConnectorBuilder.connector()
            .withPreferredProtocols(preferredProtocols(repetitionInfo))
            .withHttpClient(CrankerConnectorBuilder.createHttpClient(true).build())
            .withRouterUris(RegistrationUriSuppliers.fixedUris(registrationUri(routerServer.uri())))
            .withRoute("*")
            .withTarget(URI.create("https://test-url"))
            .withComponentName("cranker-connector-unit-test")
            .start();

        // call stop the first time
        assertTrue(connector.stop(5, TimeUnit.SECONDS));

        // return false for the second time
        assertFalse(connector.stop(1, TimeUnit.SECONDS));
    }


    @RepeatedTest(3)
    public void clientDropEarlyCanNotifyMicroservice(RepetitionInfo repetitionInfo) throws IOException {

        final ResponseInfo[] responseInfo = new ResponseInfo[1];
        final AtomicBoolean serverReceived = new AtomicBoolean(false);

        this.targetServer = httpServer()
            .addHandler(Method.GET, "/test", (request, response, pathParams) -> {
                // no response, just holding the tcp connection until client drop
                final AsyncHandle asyncHandle = request.handleAsync();
                asyncHandle.addResponseCompleteHandler(info -> {
                    log.info("http server response complete, info={}", info);
                    responseInfo[0] = info;
                });
                serverReceived.set(true);
                asyncHandle.write(ByteBuffer.wrap("hello1".getBytes()));
            })
            .start();

        log.info("http server started at {}", this.targetServer.uri());

        this.crankerRouter = crankerRouter()
            .withSupportedCrankerProtocols(List.of("cranker_3.0", "cranker_1.0"))
            .start();

        this.routerServer = httpServer()
            .withHttpPort(0)
            .addHandler(crankerRouter.createRegistrationHandler())
            .addHandler(crankerRouter.createHttpHandler())
            .start();

        this.connector = CrankerConnectorBuilder.connector()
            .withPreferredProtocols(preferredProtocols(repetitionInfo))
            .withHttpClient(CrankerConnectorBuilder.createHttpClient(true).build())
            .withRouterUris(RegistrationUriSuppliers.fixedUris(registrationUri(routerServer.uri())))
            .withRoute("*")
            .withTarget(targetServer.uri())
            .withComponentName("cranker-connector-unit-test")
            .start();

        waitForRegistration("*", connector.connectorId(), 2, crankerRouter);


        String host = this.routerServer.uri().getHost();
        int port = this.routerServer.uri().getPort();
        String path = "/test";

        try (Socket socket = new Socket(host, port);
             OutputStreamWriter writer = new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8)) {
            writer.write("GET " + path + " HTTP/1.1\r\n");
            writer.write("Host: " + host + "\r\n");
            writer.write("User-Agent: Mozilla/5.0\r\n");
            writer.write("Connection: Close\r\n");
            writer.write("\r\n");
            writer.flush();

            assertEventually(serverReceived::get, is(true));

            // client sending request and close it early
            writer.close();
            socket.close();

            AssertUtils.assertEventually(() -> responseInfo[0] != null && !responseInfo[0].completedSuccessfully(), is(true), 10, 100);
        }
    }

    @RepeatedTest(3)
    public void connectorStopCanNotifyServiceAndClient(RepetitionInfo repetitionInfo) {

        final ResponseInfo[] responseInfo = new ResponseInfo[1];
        final AtomicBoolean serverReceived = new AtomicBoolean(false);

        this.targetServer = httpServer()
            .addHandler(Method.GET, "/test", (request, response, pathParams) -> {
                // no response, just holding the tcp connection until client drop
                final AsyncHandle asyncHandle = request.handleAsync();
                asyncHandle.addResponseCompleteHandler(info -> {
                    log.info("http server response complete, info={}", info);
                    responseInfo[0] = info;
                });
                serverReceived.set(true);
                asyncHandle.write(ByteBuffer.wrap("hello1".getBytes()));
            })
            .start();

        log.info("http server started at {}", this.targetServer.uri());

        this.crankerRouter = crankerRouter()
            .withSupportedCrankerProtocols(List.of("cranker_3.0", "cranker_1.0"))
            .start();

        this.routerServer = httpServer()
            .withHttpPort(0)
            .addHandler(crankerRouter.createRegistrationHandler())
            .addHandler(crankerRouter.createHttpHandler())
            .start();

        this.connector = CrankerConnectorBuilder.connector()
            .withPreferredProtocols(preferredProtocols(repetitionInfo))
            .withHttpClient(CrankerConnectorBuilder.createHttpClient(true).build())
            .withRouterUris(RegistrationUriSuppliers.fixedUris(registrationUri(routerServer.uri())))
            .withRoute("*")
            .withTarget(targetServer.uri())
            .withComponentName("cranker-connector-unit-test")
            .start();

        waitForRegistration("*", connector.connectorId(), 2, crankerRouter);

        String host = this.routerServer.uri().getHost();
        int port = this.routerServer.uri().getPort();
        String path = "/test";

        // start a client and wait for exception
        AtomicBoolean isClientCompleted = new AtomicBoolean(false);
        new Thread(() -> {
            try (Socket socket = new Socket(host, port);
                 OutputStreamWriter writer = new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
                writer.write("GET " + path + " HTTP/1.1\r\n");
                writer.write("Host: " + host + "\r\n");
                writer.write("User-Agent: Mozilla/5.0\r\n");
                writer.write("Connection: Close\r\n");
                writer.write("\r\n");
                writer.flush();


                String line;
                while ((line = reader.readLine()) != null) {
                    log.info(line);
                }
                isClientCompleted.set(true);
            } catch (Exception exception) {
                isClientCompleted.set(true);
            }
        }).start();

        // assert request already arrived server
        assertEventually(serverReceived::get, is(true));

        // close connector while request still inflight
        log.info("connector stopping");
        final boolean isSuccess = connector.stop(1, TimeUnit.SECONDS);
        log.info("connector stopped, isSuccess={}", isSuccess);
        assertThat(isSuccess, is(false));

        // microservice and client both aware
        AssertUtils.assertEventually(() -> responseInfo[0] != null && !responseInfo[0].completedSuccessfully(), is(true), 10, 100);
        AssertUtils.assertEventually(isClientCompleted::get, is(true));

    }

}
