package com.hsbc.cranker.connector;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullSource;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.time.Duration;
import java.util.Optional;

import static java.net.http.HttpClient.Version.HTTP_1_1;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class TargetRequestTest {
    // Initialize the connector's restricted-header policy before constructing requests.
    private static final boolean HOST_SUPPORTED = !HttpUtils.DISALLOWED_REQUEST_HEADERS.contains("host");

    @ParameterizedTest
    @NullSource
    @EnumSource(HttpClient.Version.class)
    void requestsWithoutHostAreUnchanged(HttpClient.Version version) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create("https://target.example/path"));
        if (version != null) builder.version(version);
        HttpRequest request = builder.build();

        assertSame(request, HttpUtils.prepareRequestForTarget(request));
        assertEquals(Optional.ofNullable(version), request.version());
        assertTrue(request.headers().allValues("host").isEmpty());
    }

    @ParameterizedTest
    @NullSource
    @EnumSource(HttpClient.Version.class)
    void forcingHttp1PreservesRequestSettings(HttpClient.Version version) {
        assumeTrue(HOST_SUPPORTED);
        HttpRequest.BodyPublisher body = HttpRequest.BodyPublishers.ofString("request body");
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create("https://target.example:8443/changed?q=value"))
            .method("PATCH", body)
            .header("HoSt", "public.example")
            .header("X-Multiple", "one").header("X-Multiple", "two")
            .header("Content-Type", "text/plain")
            .expectContinue(true)
            .timeout(Duration.ofSeconds(7));
        if (version != null) builder.version(version);
        HttpRequest request = builder.build();

        HttpRequest result = HttpUtils.prepareRequestForTarget(request);

        assertEquals(Optional.of(HTTP_1_1), result.version());
        assertEquals(request.uri(), result.uri());
        assertEquals(request.method(), result.method());
        assertSame(body, result.bodyPublisher().orElseThrow());
        assertEquals(request.headers(), result.headers());
        assertEquals(request.timeout(), result.timeout());
        assertEquals(request.expectContinue(), result.expectContinue());
        assertEquals(Optional.ofNullable(version), request.version(), "The listener's request remains immutable");
        if (version == HTTP_1_1) assertSame(request, result);
        assertSame(result, HttpUtils.prepareRequestForTarget(result));
    }

    @Test
    void hostOnBodylessRequestAlsoForcesHttp1() {
        assumeTrue(HOST_SUPPORTED);
        HttpRequest request = HttpRequest.newBuilder(URI.create("https://target.example/"))
            .header("Host", "target.example").GET().build();

        HttpRequest result = HttpUtils.prepareRequestForTarget(request);

        assertEquals(Optional.of(HTTP_1_1), result.version());
        assertEquals("GET", result.method());
        assertEquals(request.headers(), result.headers());
        assertEquals(0, result.bodyPublisher().orElseThrow().contentLength());
    }
}
