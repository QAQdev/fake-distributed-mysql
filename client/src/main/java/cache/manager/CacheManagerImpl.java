package cache.manager;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CacheManagerImpl implements CacheManager {
    private Map<String, List<String>> cache;
    private static final String log_File = "./log.txt";
    private static final String serial_File = "./serialized_Map.out";

    public CacheManagerImpl() {
        try {
            this.cache = Map_Serializer.deserialize_Map(serial_File);
        } catch (IOException e) {
            this.cache = new HashMap<>();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> get_Cache(String table_name) {
        return cache.get(table_name);
    }

    @Override
    public void set_Cache(String table_name, String[] server_IPs) {
        List<String> region_IP = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        for (String server_IP : server_IPs) {
            if (server_IP.length() == 0) continue;
            String real_IP = server_IP.substring(0, server_IP.lastIndexOf(":"));
            region_IP.add(real_IP);
            sb.append(real_IP);
            sb.append(' ');
        }
        cache.put(table_name, region_IP);
        add_Log("Add cache: " + table_name + " - " + sb.toString().trim());
    }

    @Override
    public void shutdown() {
        try {
            Map_Serializer.serialize_Map(cache, serial_File);
        } catch (IOException ignored) {
        }
    }

    public void add_Log(String content) {
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(log_File, true)));
            out.write(content + "\r\n");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    static class Map_Serializer {
        public static void serialize_Map(Map<String, List<String>> map, String file_Path) throws IOException {
            FileOutputStream fos = new FileOutputStream(file_Path);
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(map);
            oos.close();
        }

        public static Map<String, List<String>> deserialize_Map(String file_Path) throws IOException, ClassNotFoundException {
            FileInputStream fis = new FileInputStream(file_Path);
            ObjectInputStream ois = new ObjectInputStream(fis);
            Map<String, List<String>> map = (HashMap<String, List<String>>) ois.readObject();
            ois.close();
            return map;
        }
    }
}
