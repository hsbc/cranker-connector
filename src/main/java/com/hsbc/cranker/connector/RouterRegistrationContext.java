package com.hsbc.cranker.connector;

import java.net.http.WebSocket;

/**
 * A context for router registration
 */
public interface RouterRegistrationContext {

    /**
     * Get the websocket builder for the router registration, the modification on the builder will take effect
     * when creating the websocket connection.
     *
     * @return The websocket builder
     */
    WebSocket.Builder getWebsocketBuilder();

    /**
     * Get the router registration, which contains information about connections to a router
     *
     * @return the router registration
     */
    RouterRegistration getRouterRegistration();
}

class RouterRegistrationContextImpl implements RouterRegistrationContext {

    private final WebSocket.Builder websocketBuilder;
    private final RouterRegistration routerRegistration;

    public RouterRegistrationContextImpl(WebSocket.Builder websocketBuilder, RouterRegistration routerRegistration) {
        this.websocketBuilder = websocketBuilder;
        this.routerRegistration = routerRegistration;
    }

    @Override
    public WebSocket.Builder getWebsocketBuilder() {
        return websocketBuilder;
    }

    @Override
    public RouterRegistration getRouterRegistration() {
        return routerRegistration;
    }
}
