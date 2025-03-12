package com.hsbc.cranker.connector;

/**
 * A listener for registration events
 */
public interface RegistrationEventListener {

    /**
     * Called before the connector registers to the router
     * <p>This callback can be used to inspect or change registration context.</p>
     *
     * @param context The builder for websocket request, the change on the builder
     *                will be applied when creating the websocket connection.
     */
    default void beforeRegisterToRouter(RouterRegistrationContext context) {
    }
}
