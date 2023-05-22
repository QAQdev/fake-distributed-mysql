package cache.manager;

import java.util.List;

public interface CacheManager {
    List<String> get_Cache(String table_name);

    void set_Cache(String table_name, String[] server_IPs);

    void shutdown();
}
