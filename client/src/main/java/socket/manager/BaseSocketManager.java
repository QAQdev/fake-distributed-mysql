package socket.manager;

import java.io.*;
import java.net.Socket;

public abstract class BaseSocketManager {
    protected Socket socket = null;
    protected BufferedReader input = null;
    protected PrintWriter output = null;
    protected boolean is_Running = false;
    protected Thread infoListener = null;

    protected static final String log_File = "./log.txt";

    protected String IP = "172.20.10.7";
    protected int PORT = 1234;

    BaseSocketManager() {
    }

    public void set_IP(String IP) {
        this.IP = IP;
    }

    public void set_PORT(int PORT) {
        this.PORT = PORT;
    }

    public Socket get_Socket() {
        return socket;
    }

    public void listen() {
        infoListener = new InfoListener();
        infoListener.start();
    }

    public void shutdown() throws IOException {
        socket.shutdownInput();
        socket.shutdownOutput();
        socket.close();
        is_Running = false;
        infoListener.interrupt();
    }

    public void close_Socket() throws IOException {
        socket.shutdownInput();
        socket.shutdownOutput();
        socket.close();
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

    protected abstract void receive() throws IOException, InterruptedException;

    class InfoListener extends Thread {
        @Override
        public void run() {
            System.out.println("New Info >>> Listening thread for " + socket.getInetAddress().getHostAddress() + " startup!");
            add_Log("New Info >>> Listening thread for " + socket.getInetAddress().getHostAddress() + " startup!");
            while (is_Running) {
                if (socket.isClosed() || socket.isInputShutdown() || socket.isOutputShutdown()) {
                    is_Running = false;
                    break;
                }
                try {
                    receive();
                } catch (IOException | InterruptedException e) {

                }
                try {
                    sleep(100);
                } catch (InterruptedException | NullPointerException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
