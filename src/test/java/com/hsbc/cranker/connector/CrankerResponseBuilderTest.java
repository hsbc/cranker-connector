package com.hsbc.cranker.connector;

import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class CrankerResponseBuilderTest {

    @Test
    void canRemoveHttp2ResponsePseudoHeader() {

        CrankerResponseBuilder builder = new CrankerResponseBuilder()
            .withResponseStatus(200)
            .withResponseReason("OK")
            .withHeader(":status", "200")
            .withHeader("content-length", "11")
            .withHeader("cookie", "a=a1")
            .withHeader("cookie", "b=b2");

        assertThat(builder.build(), is(Stream.of(
                "HTTP/1.1 200 OK",
                "content-length:11",
                "cookie:a=a1",
                "cookie:b=b2"
            ).map(item -> item + "\n")
            .collect(Collectors.joining(""))));
    }
}
