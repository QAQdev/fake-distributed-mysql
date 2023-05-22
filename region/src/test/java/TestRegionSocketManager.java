import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class TestRegionSocketManager {
    public static void main(String[] args) throws Exception {
//        ClientSocketManager clientSocketManager = new ClientSocketManager(8080);
//        Thread clientSocketThread = new Thread(clientSocketManager);
//        clientSocketThread.start();

        ServerSocket serverSocket = new ServerSocket(1234);
        Thread master = new Thread(() -> {
            Socket socket;
            do {
                try {
                    socket = serverSocket.accept();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } while (socket == null);

            BufferedWriter bufferedWriter;
            try {
                bufferedWriter = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

//            String cmd = "<master-region>[1]10.186.2.44 app_pandas_commit_by_day\n";
//            String cmd = "<master-region>[2]insert into user values(\"hyx\", \"123\")\n";
            String cmd = "<master-region>[2]drop table test;\n";
            while (true) {
                try {
                    bufferedWriter.write(cmd);
                    bufferedWriter.flush();
                    Thread.sleep(100 * 1000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        master.start();

        RegionManager regionManager = new RegionManager();
        regionManager.run();
    }
}
