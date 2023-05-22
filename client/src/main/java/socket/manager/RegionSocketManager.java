package socket.manager;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

public class RegionSocketManager extends BaseSocketManager {
    private final MasterSocketManager master_Socket_Manager;
    private Deque<Map<String, String>> records;
    private Map<String, Integer> maxlen;

    public RegionSocketManager(MasterSocketManager master_Socket_Manager) {
        this.master_Socket_Manager = master_Socket_Manager;
        records = new ArrayDeque<>();
        maxlen = new HashMap<>();
    }

    public void connect() throws IOException {
        if (socket != null && socket.getInetAddress().getHostAddress().equals(IP) && socket.getPort() == PORT) return;
        if (socket != null) close_Socket();
        socket = new Socket(IP, PORT);
        input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        output = new PrintWriter(socket.getOutputStream(), true);
        is_Running = true;
        maxlen.clear();
        listen();
    }

    public void add_Record(Map<String, String> record) {
        records.offer(record);
    }

    /**
     * 向Region请求进行读操作
     *
     * @param sql 要执行的sql语句
     */
    public void send_P1(String sql) {
        output.println("<client-region>[1] " + sql);
        add_Log("<client-region>[1] " + sql);
    }

    @Override
    /*protected void receive() throws IOException {
        String line = "";
        if (socket.isClosed() || socket.isInputShutdown() || socket.isOutputShutdown()) {
            System.out.println("New Info >>> Region-Socket is closed!");
            add_Log("New Info >>> Region-Socket is closed!");
        } else {
            line = input.readLine();
        }
        if (line != null) {
            if (line.endsWith("table not exist")) {
                System.out.println("New Info >>> Client cache failure!");
                add_Log("New Info >>> Client cache failure!");
                Map<String, String> record = records.peekLast();
                master_Socket_Manager.send_P1(record.get("sql"), record.get("table"));
            } else {
                System.out.println("New Info >>> Received from Region: " + line);
                add_Log("New Info >>> Received from Region: " + line);
            }
        }
    }*/
    protected void receive() throws IOException {
        String jsonStr = null;
        if (socket.isClosed() || socket.isInputShutdown() || socket.isOutputShutdown()) {
            System.out.println("New Info >>> Region-Socket is closed!");
            add_Log("New Info >>> Region-Socket is closed!");
        } else {
            String s = "";
            StringBuilder sb = new StringBuilder();
            if (!(s = input.readLine()).equals("")) {
                sb.append(s);
                while (!(s = input.readLine()).equals("END_OF_TRANSMISSION")) {
                    sb.append(s);
                }
                jsonStr = sb.toString();
            }
        }
        if (jsonStr != null) {
            if (jsonStr.endsWith("table not exist")) {
                System.out.println("New Info >>> Client cache failure!");
                add_Log("New Info >>> Client cache failure!");
                Map<String, String> record = records.peekLast();
                master_Socket_Manager.send_P1(record.get("sql"), record.get("table"));
            } else {
                jsonStr = jsonStr.substring(19);
                System.out.println("New Info >>> The result is shown below:");
                add_Log("New Info >>> The result is shown below:");
            }
        } else return;
        // 解析JSON字符串并打印所有字段和值
        ObjectMapper mapper = new ObjectMapper();
        JsonNode json = mapper.readTree(jsonStr);
        // 获取每个字段对应的值的最大长度
        get_Maxlen(json);
        // 打印节点的字段名
        printField(json);
        // 打印节点的值
        printNode(json);
    }

    private void get_Maxlen(JsonNode node) {
        if (node.isObject()) {
            node.fields().forEachRemaining(entry -> maxlen.put(entry.getKey(), Math.max(maxlen.getOrDefault(entry.getKey(), 0), Math.max(entry.getValue().asText().length(), entry.getKey().length()))));
        } else if (node.isArray()) {
            for (JsonNode elem : node) {
                get_Maxlen(elem);
            }
        }
    }

    private void printField(JsonNode node) {
        JsonNode tmp = (node.isObject()) ? node : node.get(0);
        StringBuilder sb = new StringBuilder();
        System.out.print("| ");
        sb.append("| ");
        tmp.fields().forEachRemaining(entry -> {
            String field = entry.getKey();
            System.out.print(field);
            sb.append(field);
            for (int i = 0; i < maxlen.get(field) - field.length() + 3; i++) {
                if (i == maxlen.get(field) - field.length() + 1) {
                    System.out.print('|');
                    sb.append('|');
                } else {
                    System.out.print(' ');
                    sb.append(' ');
                }
            }
        });
        add_Log(sb.toString());
        System.out.println();
    }

    private void printNode(JsonNode node) {
        if (node.isObject()) {
            StringBuilder sb = new StringBuilder();
            System.out.print("| ");
            sb.append("| ");
            node.fields().forEachRemaining(entry -> {
                String val = entry.getValue().asText();
                System.out.print(val);
                sb.append(val);
                for (int i = 0; i < maxlen.get(entry.getKey()) - val.length() + 3; i++) {
                    if (i == maxlen.get(entry.getKey()) - val.length() + 1) {
                        System.out.print('|');
                        sb.append('|');
                    } else {
                        System.out.print(' ');
                        sb.append(' ');
                    }
                }
            });
            System.out.println();
            add_Log(sb.toString());
        } else if (node.isArray()) {
            for (JsonNode elem : node) {
                printNode(elem);
            }
        }
    }
}
