package com.hsbc.cranker.connector;

import io.muserver.HttpsConfigBuilder;
import io.muserver.MuServer;
import io.muserver.MuServerBuilder;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLHandshakeException;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HttpUtilsTest {

    @Test
    void nonTrustAllClientThrowsExceptionWhenConnectToSelfSignedCertServer() {

        MuServer server = MuServerBuilder.httpsServer()
            .withHttpsConfig(HttpsConfigBuilder.unsignedLocalhost())
            .addHandler((request, response) -> {
                response.status(200);
                response.write("Hello");
                return true;
            })
            .start();


        assertThrows(SSLHandshakeException.class, () -> {
            HttpClient client = HttpUtils.createHttpClientBuilder(false).build();
            client.send(HttpRequest.newBuilder(server.uri()).build(), HttpResponse.BodyHandlers.ofString());
        });
    }

    @Test
    void testTrustAllClient() throws IOException, InterruptedException {

        MuServer server = MuServerBuilder.httpsServer()
            .withHttpsConfig(HttpsConfigBuilder.unsignedLocalhost())
            .addHandler((request, response) -> {
                response.status(200);
                response.write("Hello");
                return true;
            })
            .start();

        HttpClient client = HttpUtils.createHttpClientBuilder(true).build();
        HttpResponse<String> response = client.send(HttpRequest.newBuilder(server.uri()).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode(), equalTo(200));
        assertThat(response.body(), equalTo("Hello"));
    }
}