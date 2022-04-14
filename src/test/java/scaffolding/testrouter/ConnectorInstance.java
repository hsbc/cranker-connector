package scaffolding.testrouter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Gets a snapshot of information about a connector.
 */
public interface ConnectorInstance {

    /**
     * @return The IP address of the instance.
     */
    String ip();

    /**
     * @return The unique ID of the connector.
     */
    String connectorInstanceID();

    /**
     * @return The current idle connections that this connector has registered to the router.
     */
    List<ConnectorConnection> connections();

    /**
     * @return Returns <code>true</code> if this connector is on a dark mode host; otherwise <code>false</code>.
     */
    boolean darkMode();

    /**
     * @return Returns the state of this object as a map of key-value pairs.
     */
    Map<String, Object> toMap();
}

class ConnectorInstanceImpl implements ConnectorInstance {

    private final String ip;
    private final String connectorInstanceID;
    private final ArrayList<ConnectorConnection> connections;
    private final boolean darkMode;

    ConnectorInstanceImpl(String ip, String connectorInstanceID, ArrayList<ConnectorConnection> connections, boolean darkMode) {
        this.ip = ip;
        this.connectorInstanceID = connectorInstanceID;
        this.connections = connections;
        this.darkMode = darkMode;
    }

    @Override
    public String ip() {
        return ip;
    }

    @Override
    public String connectorInstanceID() {
        return connectorInstanceID;
    }

    @Override
    public List<ConnectorConnection> connections() {
        return connections;
    }

    @Override
    public boolean darkMode() {
        return darkMode;
    }

    @Override
    public Map<String, Object> toMap() {
        HashMap<String, Object> m = new HashMap<>();
        m.put("connectorInstanceID", connectorInstanceID);
        m.put("ip", ip);
        List<HashMap<String, Object>> cons = new ArrayList<>();
        for (ConnectorConnection con : connections) {
            cons.add(con.toMap());
        }
        m.put("connections", cons);
        m.put("connectionCount", cons.size());
        m.put("darkMode", darkMode);
        return m;
    }

    @Override
    public String toString() {
        return "ConnectorInstanceImpl{" +
            "ip='" + ip + '\'' +
            ", connectorInstanceID='" + connectorInstanceID + '\'' +
            ", connections=" + connections +
            '}';
    }
}
