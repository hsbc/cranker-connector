package scaffolding.testrouter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Request from router to connector
 */
class CrankerProtocolRequest {
    static final String REQUEST_HAS_NO_BODY_MARKER = "_2";
    static final String REQUEST_BODY_PENDING_MARKER = "_1";
    static final String REQUEST_BODY_ENDED_MARKER = "_3";

    /**
     * CRANKER_PROTOCOL_VERSION_1_0
     * request msg format:
     * <p>
     * ======msg without body========
     * ** GET /modules/uui-allocation/1.0.68/uui-allocation.min.js.map HTTP/1.1\n
     * ** [headers]\n
     * ** \n
     * ** endmarker
     * <p>
     * <p>
     * OR
     * <p>
     * =====msg with body part 1=======
     * ** GET /modules/uui-allocation/1.0.68/uui-allocation.min.js.map HTTP/1.1\n
     * ** [headers]\n
     * ** \n
     * ** endmarker
     * =====msg with body part 2=========
     * ** [BINRAY BODY]
     * =====msg with body part 3=======
     * ** endmarker
     */
    private static final Logger log = LoggerFactory.getLogger(CrankerProtocolRequest.class);
    public String httpMethod;
    public String dest;
    public String[] headers;
    private String endMarker;

    public CrankerProtocolRequest(String msg) {
        if (msg.equals(REQUEST_BODY_ENDED_MARKER)) {
            this.endMarker = msg;
        } else {
            String[] msgArr = msg.split("\n");
            String request = msgArr[0];
            log.debug("request >>> " + request);
            String[] bits = request.split(" ");
            this.httpMethod = bits[0];
            this.dest = bits[1];
            String[] headersArr = Arrays.copyOfRange(msgArr, 1, msgArr.length - 1);
            log.debug("headers >>> " + Arrays.toString(headersArr));
            this.headers = headersArr;
            String marker = msgArr[msgArr.length - 1];
            log.debug("marker >>> " + marker);
            this.endMarker = marker;
        }
    }

    public boolean requestBodyPending() {
        return endMarker.equals(REQUEST_BODY_PENDING_MARKER);
    }

    public boolean requestBodyEnded() {
        return endMarker.equals(REQUEST_BODY_ENDED_MARKER);
    }

    public void sendRequestToTarget(RequestCallback sendRequestCallback) {
        if (endMarker.equals(REQUEST_BODY_PENDING_MARKER) || endMarker.equals(REQUEST_HAS_NO_BODY_MARKER)) {
            log.debug("Request headers received");
            sendRequestCallback.callback();
            log.debug("Request body fully sent");
        }
    }

    public interface RequestCallback {
        void callback();
    }

    @Override
    public String toString() {
        return "CrankerProtocolRequest{" + httpMethod + " " + dest + '}';
    }
}
