package client.manager;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface ClientManager {
    void run() throws IOException, InterruptedException;

    void connect_Region(String IP, int port, String sql, Map<String, String> record) throws IOException, InterruptedException;

    void set_Cache(String table, String[] IPs);

    List<String> get_Cache(String table_name);

}
