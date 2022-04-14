package scaffolding.testrouter;

import io.muserver.Headers;

import java.util.Map;

class HeadersBuilder {

    private final Headers headers = Headers.http1Headers();

    public void appendHeader(String header, String value) {
        headers.add(header, value);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> header : headers) {
            sb.append(header.getKey()).append(':').append(header.getValue()).append('\n');
        }
        return sb.toString();
    }

    public Headers muHeaders() {
        return headers;
    }
}
