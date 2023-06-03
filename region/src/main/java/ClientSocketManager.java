import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

public class ClientSocketManager implements Runnable {

    private ServerSocket serverSocket;
    private HashMap<Socket, Thread> clientHashMap;

    private DatabaseManager databaseManager;

    public ClientSocketManager(int port, DatabaseManager databaseManager) throws IOException {
        this.serverSocket = new ServerSocket(port);
        this.clientHashMap = new HashMap<>();
        this.databaseManager = databaseManager;
    }

    @Override
    public void run() {
        try {
            while (true) {
                Thread.sleep(1000);
                // 等待与之连接的客户端
                Socket socket = serverSocket.accept();
                // 建立子线程并启动
                ClientThread clientThread = new ClientThread(socket, databaseManager);
                Thread thread = new Thread(clientThread);
                // 把子线程放入hashmap中
                this.clientHashMap.put(socket, thread);
                thread.start();
            }
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }
}

