package com.hsbc.cranker.connector;

import io.muserver.MuHandler;
import io.muserver.MuServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.muserver.MuServerBuilder.httpServer;
import static org.junit.jupiter.api.Assertions.*;

public class ConcurrentUploadTest extends BaseEndToEndTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrentUploadTest.class);

    private volatile MuHandler handler = (request, response) -> false;

    protected MuServer targetServer = httpServer()
        .addHandler((request, response) -> handler.handle(request, response))
        .start();

    private CrankerConnector connector;

    @BeforeEach
    void setUp(RepetitionInfo repetitionInfo) {
        connector = CrankerConnectorBuilder.connector()
            .withPreferredProtocols(preferredProtocols(repetitionInfo))
            .withHttpClient(CrankerConnectorBuilder.createHttpClient(true).build())
            .withRouterUris(RegistrationUriSuppliers.fixedUris(registrationUri(registrationServer.uri())))
            .withRoute("*")
            .withTarget(targetServer.uri())
            .withComponentName("cranker-connector-unit-test")
            .start();

        waitForRegistration("*", connector.connectorId(),2, crankerRouter);
    }

    @AfterEach
    public void stop() throws Exception {
        connector.stop(10, TimeUnit.SECONDS);
        targetServer.stop();
    }

    @RepeatedTest(3)
    public void postLargeBody() throws InterruptedException {
        final String body = "c".repeat(10 * 1000);
        handler = (request, response) -> {
            response.status(200);
            String text = request.readBodyAsString();
            response.write(text);
            return true;
        };

        Queue<HttpResponse<String>> responses = new ConcurrentLinkedQueue<>();
        CountDownLatch countDownLatch = new CountDownLatch(10);

        for(int i = 0; i < 10; i++) {
            new Thread(() -> {
                try {
                    HttpResponse<String> resp = testClient.send(HttpRequest.newBuilder()
                        .method("POST", HttpRequest.BodyPublishers.ofString(body))
                        .uri(crankerServer.uri())
                        .build(), HttpResponse.BodyHandlers.ofString());
                    responses.add(resp);
                    countDownLatch.countDown();
                } catch (Exception e) {
                    LOGGER.error("Concurrent request error", e);
                    responses.add(null);
                }
            }).start();
        }

        assertTrue(countDownLatch.await(10, TimeUnit.SECONDS));
        assertEquals(10, responses.size());
        for (HttpResponse<String> response: responses) {
            assertNotNull(response);
            assertEquals(200, response.statusCode());
            assertEquals(body, response.body());
        }
    }
}
