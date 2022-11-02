package com.hsbc.cranker.connector;

interface ConnectorSocketListener {

    /**
     * Called when the socket is being used for a request
     */
    void onConnectionAcquired(ConnectorSocket socket);

    /**
     * Called when the socket close
     *
     * @param socket
     * @param error null if there are no error
     */
    void onClose(ConnectorSocket socket, Throwable error);
}
