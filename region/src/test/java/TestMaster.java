import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class TestMaster {
    public static void main(String[] args) throws IOException, InterruptedException {
        ServerSocket serverSocket = new ServerSocket(1234);
        while (true) {
            Socket socket = serverSocket.accept();
        }
    }
}
