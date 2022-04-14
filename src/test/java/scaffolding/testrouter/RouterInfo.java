package scaffolding.testrouter;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public interface RouterInfo {

    List<ConnectorService> services();

    Map<String, Object> toMap();

    Set<DarkHost> darkHosts();
}

class RouterInfoImpl implements RouterInfo {

    private final List<ConnectorService> services;
    private final Set<DarkHost> darkHosts;

    RouterInfoImpl(List<ConnectorService> services, Set<DarkHost> darkHosts) {
        this.services = services;
        this.darkHosts = darkHosts;
    }

    @Override
    public List<ConnectorService> services() {
        return services;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> i = new HashMap<>();
        for (ConnectorService service : services) {
            i.put(service.route(), service.toMap());
        }
        return i;
    }

    @Override
    public Set<DarkHost> darkHosts() {
        return darkHosts;
    }

    @Override
    public String toString() {
        return "RouterInfoImpl{" +
            "services=" + services +
            '}';
    }

    static void addSocketData(List<ConnectorService> services, Map<String, ConcurrentLinkedQueue<RouterSocket>> sockets, Set<DarkHost> darkHosts) {
        for (Map.Entry<String, ConcurrentLinkedQueue<RouterSocket>> entry : sockets.entrySet()) {
            Map<String, ConnectorInstance> map = new HashMap<>();
            List<ConnectorInstance> instances = new ArrayList<>();
            for (RouterSocket routerSocket : entry.getValue()) {
                String connectorInstanceID = routerSocket.connectorInstanceID();
                ConnectorInstance connectorInstance = map.get(connectorInstanceID);
                if (connectorInstance == null) {
                    String ip = routerSocket.serviceAddress().getHostString();
                    boolean darkMode = routerSocket.isDarkModeOn(darkHosts);
                    connectorInstance = new ConnectorInstanceImpl(ip, connectorInstanceID, new ArrayList<>(), darkMode);
                    map.put(connectorInstanceID, connectorInstance);
                    instances.add(connectorInstance);
                }

                int port = routerSocket.serviceAddress().getPort();
                ConnectorConnection cc = new ConnectorConnectionImpl(port, routerSocket.routerSocketID);
                connectorInstance.connections().add(cc);

            }
            services.add(new ConnectorServiceImpl(entry.getKey(), instances));
        }
    }
}
