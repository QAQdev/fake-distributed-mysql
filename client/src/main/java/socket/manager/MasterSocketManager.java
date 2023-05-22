package socket.manager;

import client.manager.ClientManagerImpl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class MasterSocketManager extends BaseSocketManager {
    private final ClientManagerImpl client_Manager;
    private Map<String, String> command_Map;

    public MasterSocketManager(ClientManagerImpl client_Manager) throws IOException {
        socket = new Socket(IP, PORT);
        input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        output = new PrintWriter(socket.getOutputStream(), true);
        is_Running = true;
        this.client_Manager = client_Manager;
        command_Map = new HashMap<>();
        listen();
    }

    /**
     * 向Master查询表所在的Region的IP
     *
     * @param sql   要执行的sql语句
     * @param table 表名
     */
    public void send_P1(String sql, String table) {
        command_Map.put(table, sql);
        output.println("<client-master>[1] " + table);
        add_Log("<client-master>[1] " + table);
    }

    /**
     * 向Master请求创建新表
     *
     * @param sql   要执行的sql-create语句
     * @param table 表名
     */
    public void send_P2(String sql, String table) {
        command_Map.put(table, sql);
        output.println("<client-master>[2](" + table + ") " + sql);
        add_Log("<client-master>[2](" + table + ") " + sql);
    }

    /**
     * 向Master请求进行写操作
     *
     * @param sql   要执行的sql语句
     * @param table 表名
     */
    public void send_P3(String sql, String table) {
        output.println("<client-master>[3](" + table + ") " + sql);
        add_Log("<client-master>[3](" + table + ") " + sql);
    }

    @Override
    protected void receive() throws IOException, InterruptedException {
        String line = null;
        if (socket.isClosed() || socket.isInputShutdown() || socket.isOutputShutdown()) {
            System.out.println("New Info >>> Master-Socket is closed!");
            add_Log("New Info >>> Master-Socket is closed!");
        } else {
            line = input.readLine();
        }
        if (line != null) {
            System.out.println("New Info >>> Received from Master: " + line);
            add_Log("New Info >>> Received from Master: " + line);
            if (line.startsWith("<master-client>[1]")) {
                if (line.endsWith("table not exist")) return;
                line = line.substring(19);
                String table = line.substring(0, line.indexOf(' '));
                line = line.substring(line.indexOf(' ') + 1);
                String[] ips = line.split(" ");
                this.client_Manager.set_Cache(table, ips);
                Random random = new Random();
                List<String> real_IPs = this.client_Manager.get_Cache(table);
                String chosen_IP = real_IPs.get(random.nextInt(real_IPs.size()));
                int port = Integer.parseInt(chosen_IP.substring(chosen_IP.indexOf(":") + 1));
                chosen_IP = chosen_IP.substring(0, chosen_IP.indexOf(":"));
                this.client_Manager.connect_After_Lookup(chosen_IP, port, command_Map.get(table), table);
            } else if (line.startsWith("<master-client>[2]")) {
                line = line.substring(19);
                String table = line.substring(0, line.indexOf(' '));
                line = line.substring(line.indexOf(' ') + 1);
                String[] ips = line.split(" ");
                this.client_Manager.set_Cache(table, ips);
            }
        }
    }
}