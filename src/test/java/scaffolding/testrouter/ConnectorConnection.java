package scaffolding.testrouter;

import java.util.HashMap;

public interface ConnectorConnection {

    String socketID();

    int port();

    HashMap<String, Object> toMap();
}

class ConnectorConnectionImpl implements ConnectorConnection {

    private final int port;
    private final String socketID;

    ConnectorConnectionImpl(int port, String socketID) {
        this.port = port;
        this.socketID = socketID;
    }

    @Override
    public String socketID() {
        return socketID;
    }

    @Override
    public int port() {
        return port;
    }

    @Override
    public HashMap<String, Object> toMap() {
        HashMap<String, Object> m = new HashMap<>();
        m.put("socketID", socketID);
        m.put("port", port);
        return m;
    }

    @Override
    public String toString() {
        return "ConnectorConnectionImpl{" +
            "socketID='" + socketID + '\'' +
            '}';
    }
}
