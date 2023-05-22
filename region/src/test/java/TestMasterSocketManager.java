public class TestMasterSocketManager {
    public static void main(String[] args) throws Exception {
        MasterSocketManager masterSocketManager = new MasterSocketManager("127.0.0.1", 1234, null, null);
        Thread thread = new Thread(masterSocketManager);
        thread.start();
    }
}
