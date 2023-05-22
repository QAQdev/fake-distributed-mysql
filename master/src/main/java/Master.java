import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
@Slf4j
public class Master {
    BlockingQueue<WriteTask> writeTasks;
    BlockingQueue<WriteResult> writeResults;
    TableManager tableManager;
    SocketManager socketManager;
    ZKManager zkManager;
    ClientProcessor clientProcessor;
    WriteHandler writeHandler;
    public SocketAddress GetIp (int port) throws UnknownHostException {

//        String ipAddress = InetAddress.getLocalHost().getHostAddress();
        String ipAddress = "172.20.10.7";
//        log.info("masterrrrrrrrrrrrrrrrrr"+ipAddress);
//        String ipAddress = "1/27.0.0.1";
        SocketAddress addr = new InetSocketAddress(ipAddress,port);
        return addr;

    }
    public Master(){
        SocketAddress addr  = null;
        try{
            addr = GetIp(1234);
            writeTasks = new LinkedBlockingQueue<>();
            writeResults = new LinkedBlockingQueue<>();
            tableManager = new TableManager();
            //向socketManager传入clientProcessor->handler
            clientProcessor = new ClientProcessor(writeTasks,writeResults,tableManager,null);
            socketManager = new SocketManager(addr,writeTasks,clientProcessor,tableManager);
            clientProcessor.init(socketManager);
//            clientProcessor.socketManager = socketManager;
            zkManager = new ZKManager(tableManager,socketManager,writeResults);

            writeHandler= new WriteHandler(writeTasks,zkManager);

        }catch (Exception e){
            System.out.println("here");
            System.out.println(e);
        }


    }
    public void Init(){
        zkManager.startZookeeper();
        log.info("start write Handler");
        new Thread(writeHandler).start();
        new Thread(socketManager).start();


    }
    public static void main(String[] args) {

       Master master = new Master();

       master.Init();

        while (true)
        {

        }
    }
}
