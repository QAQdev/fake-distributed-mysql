import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.net.Socket;

public class TestClient {
    public static void main(String[] args) throws IOException {
        Socket socket = new Socket("127.0.0.1", 8080);
        BufferedReader bufferedReader = new BufferedReader(new java.io.InputStreamReader(socket.getInputStream()));
        BufferedWriter bufferedWriter = new BufferedWriter(new java.io.OutputStreamWriter(socket.getOutputStream()));

        bufferedWriter.write("select * from app_commit_history;");
        bufferedWriter.flush();
    }
}