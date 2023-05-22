//import java.io.IOException;
//import java.net.ServerSocket;
//
//public class Test {
//    public static void main(String[] args) throws Exception {
//        ClientSocketManager clientSocketManager = new ClientSocketManager(8080);
//        Thread clientSocketThread = new Thread(clientSocketManager);
//        clientSocketThread.start();
//
//        ServerSocket serverSocket = new ServerSocket(1234);
//        Thread master = new Thread(() -> {
//            while (true) {
//                try {
//                    serverSocket.accept();
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//        });
//        master.start();
//
//        MasterSocketManager masterSocketManager = new MasterSocketManager("127.0.0.1", 1234, null, null);
//        Thread masterSocketThread = new Thread(masterSocketManager);
//        masterSocketThread.start();
//    }
//}
