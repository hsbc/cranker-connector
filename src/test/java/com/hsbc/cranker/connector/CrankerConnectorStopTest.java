package com.hsbc.cranker.connector;


import io.muserver.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scaffolding.testrouter.CrankerRouter;

import java.io.PrintWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hsbc.cranker.connector.BaseEndToEndTest.registrationUri;
import static com.hsbc.cranker.connector.BaseEndToEndTest.startConnectorAndWaitForRegistration;
import static io.muserver.MuServerBuilder.httpsServer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;
import static scaffolding.Action.swallowException;
import static scaffolding.AssertUtils.assertEventually;
import static scaffolding.testrouter.CrankerRouterBuilder.crankerRouter;

public class CrankerConnectorStopTest {

    private static final Logger log = LoggerFactory.getLogger(CrankerConnectorStopTest.class);

    private final HttpClient httpClient = HttpUtils.createHttpClientBuilder(true)
        .version(HttpClient.Version.HTTP_2)
        .build();
    private CrankerRouter crankerRouter;
    private MuServer targetServer;
    private MuServer routerServer;
    private CrankerConnector connector;

    private AtomicInteger counter = new AtomicInteger(0);
    private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(10, r -> new Thread(r, "test-pool" + counter.incrementAndGet()));

    @AfterEach
    public void after() {
        if (connector != null) swallowException(() -> connector.stop(10, TimeUnit.SECONDS));
        if (targetServer != null) swallowException(targetServer::stop);
        if (routerServer != null) swallowException(routerServer::stop);
        if (crankerRouter != null) swallowException(crankerRouter::stop);
        if (executorService != null) swallowException(executorService::shutdownNow);
    }

    @Test
    void requestOnTheFlyShouldCompleteSuccessfullyWithinTimeout() throws ExecutionException, InterruptedException, TimeoutException {

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
            .withConnectorAcquireAttempts(4, 100)
            .start();

        this.routerServer = httpsServer()
            .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(false))
            .addHandler(crankerRouter.createRegistrationHandler())
            .addHandler(crankerRouter.createHttpHandler())
            .start();

        this.connector = startConnectorAndWaitForRegistration(crankerRouter, "*", targetServer, 2, this.routerServer);

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

    @Test
    void requestOnTheFlyShouldCompleteSuccessfullyWithinTimeout_LongQuery() throws ExecutionException, InterruptedException, TimeoutException {

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
                        Thread.sleep(1000L);
                    }
                }
            })
            .start();

        this.crankerRouter = crankerRouter()
            .withConnectorAcquireAttempts(4, 100)
            .start();

        this.routerServer = httpsServer()
            .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(false))
            .addHandler(crankerRouter.createRegistrationHandler())
            .addHandler(crankerRouter.createHttpHandler())
            .start();

        this.connector = startConnectorAndWaitForRegistration(crankerRouter, "*", targetServer, 2, this.routerServer);


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


    @Test
    void getTimeoutExceptionIfExceedTimeout() throws ExecutionException, InterruptedException, TimeoutException {

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
                        System.out.println(throwable);
                        serverExceptionCounter.incrementAndGet();
                    }
                }, 3, TimeUnit.SECONDS);
            })
            .start();

        this.crankerRouter = crankerRouter()
            .withConnectorAcquireAttempts(4, 100)
            .start();

        this.routerServer = httpsServer()
            .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(false))
            .addHandler(crankerRouter.createRegistrationHandler())
            .addHandler(crankerRouter.createHttpHandler())
            .start();

        this.connector = startConnectorAndWaitForRegistration(crankerRouter, "*", targetServer, 2, this.routerServer);

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

    @Test
    public void throwIllegalStateExceptionWhenCallingStopBeforeCallingStart() {

        this.targetServer = httpsServer()
            .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(true))
            .addHandler(Method.GET, "/test", (request, response, pathParams) -> {
                response.write("hello world");
            })
            .start();

        this.crankerRouter = crankerRouter()
            .withConnectorAcquireAttempts(4, 100)
            .start();

        this.routerServer = httpsServer()
            .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(false))
            .addHandler(crankerRouter.createRegistrationHandler())
            .addHandler(crankerRouter.createHttpHandler())
            .start();

        this.connector = CrankerConnectorBuilder.connector()
            .withHttpClient(CrankerConnectorBuilder.createHttpClient(true).build())
            .withRouterUris(RegistrationUriSuppliers.fixedUris(URI.create("wss://localhost:1234")))
            .withRoute("*")
            .withTarget(URI.create("https://test-url"))
            .withComponentName("cranker-connector-unit-test")
            .build(); // not start


        // call before start
        Exception exception = assertThrows(IllegalStateException.class, () -> connector.stop(1, TimeUnit.SECONDS));

        assertExceptionMessage(exception);
    }


    @Test
    public void throwIllegalStateExceptionWhenCallingStopMultipleTime() {

        this.targetServer = httpsServer()
            .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(true))
            .addHandler(Method.GET, "/test", (request, response, pathParams) -> {
                response.write("hello world");
            })
            .start();

        this.crankerRouter = crankerRouter()
            .withConnectorAcquireAttempts(4, 100)
            .start();

        this.routerServer = httpsServer()
            .withHttp2Config(Http2ConfigBuilder.http2Config().enabled(false))
            .addHandler(crankerRouter.createRegistrationHandler())
            .addHandler(crankerRouter.createHttpHandler())
            .start();

        this.connector = CrankerConnectorBuilder.connector()
            .withHttpClient(CrankerConnectorBuilder.createHttpClient(true).build())
            .withRouterUris(RegistrationUriSuppliers.fixedUris(registrationUri(routerServer.uri())))
            .withRoute("*")
            .withTarget(URI.create("https://test-url"))
            .withComponentName("cranker-connector-unit-test")
            .start();

        // call stop the first time
        connector.stop(10, TimeUnit.SECONDS);

        // second time will throw exception
        Exception exception = assertThrows(IllegalStateException.class, () -> connector.stop(10, TimeUnit.SECONDS));

        assertExceptionMessage(exception);
    }

    private void assertExceptionMessage(Exception exception) {
        String expectedMessage = "Cannot call stop() when the connector is not running. Did you call stop() twice?";
        String actualMessage = exception.getMessage();
        assertEquals(expectedMessage, actualMessage);
    }
}
