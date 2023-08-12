package com.hsbc.cranker.connector;

interface ConnectorSocketListener {

    /**
     * Called when the socket is being used for a request
     * @param socket ConnectorSocket being acquired
     */
    void onConnectionAcquired(ConnectorSocket socket);

    /**
     * Called when the socket close
     *
     * @param socket ConnectorSocket on close
     * @param error null if there are no error
     */
    void onClose(ConnectorSocket socket, Throwable error);
}
