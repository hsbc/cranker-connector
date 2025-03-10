package com.hsbc.cranker.connector;

import java.net.http.WebSocket;

/**
 * A listener for registration events
 */
public interface RegistrationEventListener {

    /**
     * Called before the connector registers to the router
     * <p>This callback can be used to inspect or change the request before it register to router.
     * For example the auth header can be supplied in this callback</p>
     *
     * @param builder The builder for websocket request, the change on the builder
     *                will be applied when creating the websocket connection.
     */
    default void beforeRegisterToRouter(WebSocket.Builder builder) {
    }
}
