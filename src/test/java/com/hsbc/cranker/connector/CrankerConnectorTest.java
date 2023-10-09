package com.hsbc.cranker.connector;


import io.muserver.ContentTypes;
import io.muserver.MuHandler;
import io.muserver.MuServer;
import io.muserver.handlers.ResourceHandlerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;
import scaffolding.ByteUtils;
import scaffolding.StringUtils;

import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.muserver.MuServerBuilder.httpServer;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static scaffolding.AssertUtils.assertEventually;
import static scaffolding.StringUtils.randomAsciiStringOfLength;

public class CrankerConnectorTest extends BaseEndToEndTest {

    private volatile MuHandler handler = (request, response) -> false;

    protected MuServer targetServer = httpServer()
        .addHandler((request, response) -> handler.handle(request, response))
        .start();

    private CrankerConnector connector;

    @BeforeEach
    void setUp(RepetitionInfo repetitionInfo) {
        final List<String> preferredProtocols = preferredProtocols(repetitionInfo);
        connector = CrankerConnectorBuilder.connector()
            .withPreferredProtocols(preferredProtocols)
            .withHttpClient(CrankerConnectorBuilder.createHttpClient(true).build())
            .withRouterUris(RegistrationUriSuppliers.fixedUris(registrationUri(registrationServer.uri())))
            .withRoute("*")
            .withTarget(targetServer.uri())
            .withComponentName("cranker-connector-unit-test")
            .start();

        waitForRegistration("*", connector.connectorId(), 2, crankerRouter);

        assertEventually(
            () -> new ArrayList<>(connector.routers().get(0).idleSockets()).get(0).version(),
            equalTo(preferredProtocols.get(0)));
    }

    @AfterEach
    public void stop() throws Exception {
        assertThat(connector.stop(10, TimeUnit.SECONDS), is(true));
        targetServer.stop();
    }

    @RepeatedTest(3)
    public void postingBodiesWorks() throws Exception {
        final String[] contentLength = new String[1];
        handler = (request, response) -> {
            contentLength[0] = request.headers().get("content-length");
            response.status(210);
            response.headers().set("method", request.method());
            String text = request.readBodyAsString();
            response.write(text);
            return true;
        };

        String body = randomAsciiStringOfLength(100000);
        HttpResponse<String> resp = testClient.send(HttpRequest.newBuilder()
            .method("POST", HttpRequest.BodyPublishers.ofString(body))
            .uri(crankerServer.uri())
            .build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(210, resp.statusCode());
        assertEquals(body, resp.body());
        assertEventually(() -> contentLength[0], equalTo("100000"));
    }

    @RepeatedTest(3)
    public void getRequestsWork() throws Exception {

        handler = (request, response) -> {
            response.contentType(ContentTypes.TEXT_PLAIN_UTF8);
            response.sendChunk("This ");
            response.sendChunk("is ");
            response.sendChunk("a ");
            response.sendChunk("call from " + request.headers().get("user-agent"));
            return true;
        };

        for (int i = 0; i < 10; i++) {
            HttpResponse<String> resp = testClient.send(HttpRequest.newBuilder()
                .uri(crankerServer.uri().resolve("/something"))
                .header("user-agent", "cranker-connector-test-client-" + i)
                .build(), HttpResponse.BodyHandlers.ofString());
            assertEquals("This is a call from cranker-connector-test-client-" + i, resp.body());
        }
    }

    @RepeatedTest(3)
    public void getRequestsWork_largeResponse() throws Exception {

        final String text = StringUtils.randomStringOfLength(100000);
        handler = (request, response) -> {
            String agent = request.headers().get("user-agent");
            response.contentType(ContentTypes.TEXT_PLAIN_UTF8);
            response.write(text + agent);
            return true;
        };

        for (int i = 0; i < 10; i++) {
            HttpResponse<String> resp = testClient.send(HttpRequest.newBuilder()
                .uri(crankerServer.uri().resolve("/something"))
                .header("user-agent", "cranker-connector-test-client-" + i)
                .build(), HttpResponse.BodyHandlers.ofString());
            assertThat(resp.statusCode(), is(200));
            assertEquals(text + "cranker-connector-test-client-" + i, resp.body());
        }
    }


    @RepeatedTest(3)
    public void forwardHeaderSetCorrectly() throws Exception {

        handler = (request, response) -> {
            response.write(request.headers().get("forwarded"));
            return true;
        };

        HttpResponse<String> resp = testClient.send(HttpRequest.newBuilder()
            .uri(crankerServer.uri().resolve("/something"))
            .header("user-agent", "cranker-connector-test-client")
            .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(resp.body(), containsString("by="));
        assertThat(resp.body(), containsString("for="));
        assertThat(resp.body(), containsString("host="));
        assertThat(resp.body(), containsString("proto="));
    }

    @RepeatedTest(3)
    public void ifTheTargetReturnsGzippedContentThenItIsProxiedCompressed() throws Exception {
        String body = randomAsciiStringOfLength(100000);
        handler = (request, response) -> {
            response.contentType(ContentTypes.TEXT_PLAIN_UTF8);
            response.write(body);
            return true;
        };
        HttpResponse<byte[]> resp = testClient.send(HttpRequest.newBuilder()
            .uri(crankerServer.uri().resolve("/something"))
            .header("accept-encoding", "gzip")
            .build(), HttpResponse.BodyHandlers.ofByteArray());
        assertEquals(Collections.singletonList("gzip"), resp.headers().allValues("content-encoding"));
        String decompressedBody = new String(ByteUtils.decompress(resp.body()), UTF_8);
        assertEquals(body, decompressedBody);
    }

    @RepeatedTest(3)
    public void largeResponsesWork() throws Exception {
        String body = Files.readString(Path.of("src/test/resources/web/large-file.txt"));
        handler = ResourceHandlerBuilder.classpathHandler("/web").build();
        HttpResponse<String> resp = testClient.send(HttpRequest.newBuilder()
            .uri(crankerServer.uri().resolve("/large-file.txt"))
            .header("accept-encoding", "none")
            .build(), HttpResponse.BodyHandlers.ofString());
        String actual = resp.body();
        assertThat(actual.length(), is(body.length()));
        assertThat(resp.headers().allValues("content-length"), contains(String.valueOf(body.getBytes(UTF_8).length)));
        assertEquals(body, actual);
    }

    @RepeatedTest(3)
    public void toStringReturnsUsefulInfo() {
        assertThat(connector.toString(), startsWith("CrankerConnector (" + connector.connectorId() + ") registered to: [RouterRegistration"));
    }

}
