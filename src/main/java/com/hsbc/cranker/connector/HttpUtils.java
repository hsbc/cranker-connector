package com.hsbc.cranker.connector;

import javax.net.ssl.*;
import java.net.Socket;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

class HttpUtils {

    static final List<String> DISALLOWED_REQUEST_HEADERS;
    static {
        // Older JDK clients blocked headers they shouldn't. From JDK 12 these can be turned off
        // with a system property. See https://bugs.openjdk.java.net/browse/JDK-8213189
        // For JDK 11, any of "date", "from", "host", "origin", "referer", "via", "warning" will not be proxied.
        // Upgrade to JDK 12 or newer if any of these are important.
        setPropertyIfUnset("jdk.httpclient.allowRestrictedHeaders", "host,date,via,warning,from,origin,referer");

        // This detects these for the current JDK and will not forward any banned ones.
        HttpRequest.Builder builder = HttpRequest.newBuilder();
        Set<String> headersThatJDKMayReject = Set.of("date", "expect", "from", "host", "origin", "referer", "via", "warning");
        List<String> disallowed = new ArrayList<>();
        for (String header : headersThatJDKMayReject) {
            try {
                builder.header(header, "dummy");
            } catch (IllegalArgumentException e) {
                disallowed.add(header);
            }
        }
        disallowed.add("content-length"); // as the body publisher adds it
        DISALLOWED_REQUEST_HEADERS = Collections.unmodifiableList(disallowed);
    }

    static String urlEncode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    static HttpRequest prepareRequestForTarget(HttpRequest request) {
        // The router may have proxied the `host` header, however if try to set this header on
        // an HTTP2 request, then the `host` header will not match the `:authority` header, which
        // results in an illegal request. As the JDK client does not allow `:authority` to be set
        // independently of the request URL, we downgrade any HTTP2 requests to HTTP 1.1.

        if (request.headers().allValues("host").isEmpty()
            || request.version().orElse(null) == HttpClient.Version.HTTP_1_1) {
            return request;
        }

        HttpRequest.Builder builder = HttpRequest.newBuilder(request.uri())
            .version(HttpClient.Version.HTTP_1_1)
            .method(request.method(), request.bodyPublisher().orElse(HttpRequest.BodyPublishers.noBody()))
            .expectContinue(request.expectContinue());
        request.timeout().ifPresent(builder::timeout);
        request.headers().map().forEach((name, values) -> values.forEach(value -> builder.header(name, value)));
        return builder.build();
    }

    static HttpClient.Builder createHttpClientBuilder(boolean trustAll) {

        HttpClient.Builder builder = HttpClient.newBuilder();
        if (trustAll) {
            trustAll(builder);
        }
        return builder;
    }

    private static void setPropertyIfUnset(String key, String value) {
        String custom = System.getProperty(key, null);
        if (custom == null) {
            System.setProperty(key, value);
        }
    }

    private static void trustAll(HttpClient.Builder builder) {
        try {
            final TrustManager[] trustAllCerts = new TrustManager[]{
                new X509ExtendedTrustManager() {
                    public X509Certificate[] getAcceptedIssuers() {
                        return null;
                    }

                    public void checkClientTrusted(
                        final X509Certificate[] a_certificates,
                        final String a_auth_type) {
                    }

                    public void checkServerTrusted(
                        final X509Certificate[] a_certificates,
                        final String a_auth_type) {
                    }

                    public void checkClientTrusted(
                        final X509Certificate[] a_certificates,
                        final String a_auth_type,
                        final Socket a_socket) {
                    }

                    public void checkServerTrusted(
                        final X509Certificate[] a_certificates,
                        final String a_auth_type,
                        final Socket a_socket) {
                    }

                    public void checkClientTrusted(
                        final X509Certificate[] a_certificates,
                        final String a_auth_type,
                        final SSLEngine a_engine) {
                    }

                    public void checkServerTrusted(
                        final X509Certificate[] a_certificates,
                        final String a_auth_type,
                        final SSLEngine a_engine) {
                    }
                }};
            SSLContext sslContext = SSLContext.getInstance("SSL");
            sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
            builder.sslContext(sslContext);

        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            throw new RuntimeException(e);
        }
    }

}
