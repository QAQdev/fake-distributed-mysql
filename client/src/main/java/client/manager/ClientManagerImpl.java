package client.manager;

import cache.manager.CacheManagerImpl;
import socket.manager.MasterSocketManager;
import socket.manager.RegionSocketManager;

import java.io.IOException;
import java.util.*;

import static java.lang.Thread.sleep;

public class ClientManagerImpl implements ClientManager {
    private CacheManagerImpl cache_Manager;
    private MasterSocketManager master_Socket_Manager;
    private RegionSocketManager region_Socket_Manager;

    public ClientManagerImpl() throws IOException {
        this.cache_Manager = new CacheManagerImpl();
        this.master_Socket_Manager = new MasterSocketManager(this);
        this.region_Socket_Manager = new RegionSocketManager(master_Socket_Manager);
    }

    @Override
    public void run() throws IOException, InterruptedException {
        System.out.println("-----Distributed-MySQL client startup!-----");
        Scanner in = new Scanner(System.in);
        while (true) {
            String line = "";
            StringBuilder sql = new StringBuilder();
            System.out.println("New Info >>> Please enter the SQL you want to execute: ");
            while (line.isEmpty() || line.charAt(line.length() - 1) != ';') {
                line = in.nextLine().trim();
                if (line.isEmpty()) continue;
                sql.append(line).append(" ");
            }
            String command = sql.toString().trim();
            master_Socket_Manager.add_Log("User input: " + command);
            if (command.equals("exit;")) {
                master_Socket_Manager.shutdown();
                if (region_Socket_Manager.get_Socket() != null) region_Socket_Manager.shutdown();
                cache_Manager.shutdown();
                break;
            }
            Map<String, String> content_Map = interpret(command.substring(0, command.length() - 1));
            command = command.replaceAll("\"", "'");
            content_Map.put("sql", command);
            if (content_Map.containsKey("error")) {
                System.out.println("New Info >>> Please check the syntax of SQL!");
                master_Socket_Manager.add_Log("New Info >>> User input error!");
                continue;
            }
            String table = content_Map.get("table");
            if (content_Map.get("kind").equals("create")) {
                master_Socket_Manager.send_P2(command, table);
                while (cache_Manager.get_Cache(table) == null) sleep(1000);
            } else {
                List<String> region_IPs = cache_Manager.get_Cache(table);
                if (region_IPs == null) {
                    System.out.println("New Info >>> Client cache miss!");
                    master_Socket_Manager.add_Log("New Info >>> Client cache miss!");
                    master_Socket_Manager.send_P1(command, table);
                    while (cache_Manager.get_Cache(table) == null) sleep(1000);
                } else {
                    Random random = new Random();
                    String region_IP = region_IPs.get(random.nextInt(region_IPs.size()));
                    int port = Integer.parseInt(region_IP.substring(region_IP.indexOf(":") + 1));
                    region_IP = region_IP.substring(0, region_IP.indexOf(":"));
                    if (content_Map.get("kind").equals("select")) {
                        System.out.println("New Info >>> Client cache hit! The corresponding Region Server(random chose) is " + region_IP);
                        master_Socket_Manager.add_Log("New Info >>> Client cache hit! The corresponding Region Server(random chose) is " + region_IP);
                    }
                    connect_Region(region_IP, port, command, content_Map);
                    sleep(1000);
                }
            }
        }
    }

    public void connect_After_Lookup(String IP, int port, String sql, String table) throws IOException, InterruptedException {
        if (sql.startsWith("select")) {
            this.region_Socket_Manager.set_IP(IP);
            this.region_Socket_Manager.set_PORT(port);
            this.region_Socket_Manager.connect();
            sleep(100);
            region_Socket_Manager.send_P1(sql);
        } else master_Socket_Manager.send_P3(sql, table);
    }

    public void connect_Region(String IP, int port, String sql, Map<String, String> record) throws IOException, InterruptedException {
        if (sql.startsWith("select")) {
            this.region_Socket_Manager.set_IP(IP);
            this.region_Socket_Manager.set_PORT(port);
            this.region_Socket_Manager.connect();
            this.region_Socket_Manager.add_Record(record);
            sleep(100);
            if (!(!region_Socket_Manager.get_Socket().isClosed() && region_Socket_Manager.get_Socket().isConnected())) {
                System.out.println("New Info >>> Client cache failure!");
                master_Socket_Manager.add_Log("New Info >>> Client cache failure!");
                master_Socket_Manager.send_P1(sql, record.get("table"));
                sleep(1000);
                return;
            }
            region_Socket_Manager.send_P1(sql);
        } else master_Socket_Manager.send_P3(sql, record.get("table"));
    }

    @Override
    public void set_Cache(String table, String[] IPs) {
        this.cache_Manager.set_Cache(table, IPs);
    }

    @Override
    public List<String> get_Cache(String table_name) {
        return this.cache_Manager.get_Cache(table_name);
    }

    private Map<String, String> interpret(String sql) {
        Map<String, String> result = new HashMap<>();
        sql = sql.replaceAll("\\s+", " ");
        String[] words = sql.split(" ");
        if (words.length == 0) {
            result.put("error", "true");
            return result;
        }
        result.put("kind", words[0]);
        switch (words[0]) {
            case "create":
                if (words.length >= 3) result.put("table", words[2]);
                break;
            case "drop":
            case "insert":
            case "delete":
            case "truncate":
            case "alter":
                if (words.length >= 3) {
                    String name = words[2].replace("(", "").replace(")", "").replace(";", "");
                    result.put("table", name);
                }
                break;
            case "select":
                for (int i = 0; i < words.length; i++) {
                    if (words[i].equals("from") && i != words.length - 1) {
                        result.put("table", words[i + 1]);
                        break;
                    }
                }
                break;
            case "update":
                if (words.length >= 2) result.put("table", words[1]);
                break;
        }
        if (!result.containsKey("table")) {
            result.put("error", "true");
        }
        return result;
    }
}