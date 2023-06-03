import org.apache.log4j.*;
import utils.CacheUtil;
import utils.CommandHeader;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class RegionSocketManager implements Runnable {
    private Logger logger;
    private ServerSocket serverSocket;
    private Map<String, Socket> connectedSockets;

    private DatabaseManager databaseManager;

    public RegionSocketManager(int port, DatabaseManager databaseManager) throws IOException {
        this.databaseManager = databaseManager;
        serverSocket = new ServerSocket(port);
        connectedSockets = new HashMap<>();
        logger = Logger.getLogger(RegionSocketManager.class);
    }

    @Override
    public void run() {
        try {
            listen();
        } catch (Exception e) {
            logger.log(Level.INFO, e.getStackTrace());
        }
    }

    public void listen() throws IOException {
        while (true) {
            Socket socket = serverSocket.accept();
            String command = readMessage(socket);
            // 接受命令，重建表
            if (command.startsWith(CommandHeader.REGION_TO_REGION_1.value)) {
                String commandBody = command.substring(CommandHeader.MASTER_TO_REGION_1.value.length());
                logger.log(Level.INFO, "Received Region-Region command 1: " + commandBody);
                try {
                    databaseManager.reconstructTable(commandBody);
                } catch (Exception e) {
                    logger.log(Level.INFO, "Region-Region command 1 failed: " + e);
                }
            }
            // 接受返回，重建表成功
            else if (command.startsWith(CommandHeader.REGION_TO_REGION_2.value)) {
                String commandBody = command.substring(CommandHeader.REGION_TO_REGION_2.value.length());
                logger.log(Level.INFO, "Received Region-Region command 2: " + commandBody + "reconstructed!");
            }
        }
    }

    public Socket connectToRegion(String ip) throws IOException {
        if (connectedSockets.containsKey(ip)) {
            return connectedSockets.get(ip);
        } else {
            Socket socket = new Socket(ip.split(":")[0], Integer.parseInt(ip.split(":")[1]));
            connectedSockets.put(ip, socket);
            return socket;
        }
    }

    private void handleRegionCommand(String region, String command) {
        // 处理来自特定区域的命令
    }

    public String initiatingTableCopying(String command) {
        String[] commands = command.split(" ");
        try {
            Socket socket = connectToRegion(commands[0]);
            String message = CommandHeader.REGION_TO_REGION_1.value + databaseManager.showReconstructTable(commands[1]);
            sendMessage(socket, message);
            return "true";

        } catch (IOException e) {
            logger.log(Level.INFO, "In table copying, Region-Region connection failed: " + e);
            return "false";
        }
    }

    public String initiatingTableTransferring(String command) {
        String[] commands = command.split(" ");
        try {
            Socket socket = connectToRegion(commands[0]);
            String message = CommandHeader.REGION_TO_REGION_1.value + databaseManager.showReconstructTable(commands[1]);
            sendMessage(socket, message);

            String dropTableSQL = "drop table " + commands[1] + ";";
            Statement statement = databaseManager.connection.createStatement();
            statement.executeUpdate(dropTableSQL);
            statement.close();
            CacheUtil.getInstance().cancelListenTable(databaseManager.getMetaInfo());
            logger.log(Level.INFO, "Local table dropped.");
            return "true";
        } catch (Exception e) {
            logger.log(Level.INFO, "In table transferring, Region-Region connection failed: " + e);
            return "false";
        }
    }

    private void sendMessage(Socket socket, String message) throws IOException {
        OutputStream out = socket.getOutputStream();
        byte[] messageBytes = message.getBytes(StandardCharsets.UTF_8);
        byte[] lengthBytes = String.valueOf(messageBytes.length).getBytes(StandardCharsets.UTF_8);
        out.write(lengthBytes);
        out.write('\n');
        out.write(messageBytes);
        out.flush();
    }

    private String readMessage(Socket socket) throws IOException {
        InputStream in = socket.getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        int length = Integer.parseInt(reader.readLine());
        char[] buffer = new char[length];
        reader.read(buffer, 0, length);
        return new String(buffer);
    }
}
