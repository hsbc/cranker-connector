package scaffolding.testrouter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface ConnectorService {

    String route();

    List<ConnectorInstance> connectors();

    boolean isCatchAll();

    Map<String, Object> toMap();
}

class ConnectorServiceImpl implements ConnectorService {

    private final String route;
    private final List<ConnectorInstance> instances;

    ConnectorServiceImpl(String route, List<ConnectorInstance> instances) {
        this.route = route;
        this.instances = instances;
    }

    @Override
    public String route() {
        return route;
    }

    @Override
    public List<ConnectorInstance> connectors() {
        return instances;
    }

    @Override
    public boolean isCatchAll() {
        return "*".equals(route);
    }

    @Override
    public Map<String, Object> toMap() {
        HashMap<String, Object> m = new HashMap<>();
        m.put("name", route);
        m.put("isCatchAll", isCatchAll());
        List<Map<String, Object>> cons = new ArrayList<>();
        for (ConnectorInstance instance : instances) {
            cons.add(instance.toMap());
        }
        m.put("connectors", cons);
        return m;
    }

    @Override
    public String toString() {
        return "ConnectorServiceImpl{" +
            "route='" + route + '\'' +
            ", instances=" + instances +
            '}';
    }
}
