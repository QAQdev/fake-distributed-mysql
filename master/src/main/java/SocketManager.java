import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Description TODO
 **/
@Slf4j
public class SocketManager implements  Runnable{
    private ServerSocket serverSocket;
    protected TableManager tableManager;
    private BlockingQueue<WriteTask> writeTasks ;

    //地址和SocketThread的对应关系，如127.0.0.1:port,socketThread,如果是region，存的port是和master通信的port
    protected ConcurrentHashMap<String, Handler> socketThreads;
    protected ConcurrentHashMap<Handler,Integer> handlers;

    ClientProcessor clientProcessor;


    public SocketManager(SocketAddress address, BlockingQueue<WriteTask> writeTasks,ClientProcessor clientProcessor,TableManager tb) throws IOException {
        this.serverSocket = new ServerSocket();
        this.serverSocket.bind(address);
        this.socketThreads = new ConcurrentHashMap<String, Handler>();
        this.writeTasks = writeTasks;
        this.handlers = new ConcurrentHashMap<>();
        this.clientProcessor = clientProcessor;
        this.tableManager = tb;
    }
    public void run(){
        log.info("socketmanager running!");
        startService();
    }
    /**
     * 1.接受region server的连接，等待region server发送存储的表的信息
     * 2.
     */
    public void startService() {
        while (true) {
            try {
                Socket sk = serverSocket.accept();
                log.info("connected with" + sk.getInetAddress().toString()+":"+sk.getPort());

                //
                Set<String>servers = null;
                synchronized (tableManager){
                    servers = tableManager.serverTables.keySet();
                    log.info("socketmanager:"+servers.size()+" "+tableManager.serverCount());
                }
                String inaddr = sk.getInetAddress().toString();
                inaddr = inaddr.substring(1);
                boolean flag = false;
                log.info("socketmanager current server size:"+servers.size());
                // 如果是reguon，就存放ip:region=master port
                for(String s: servers){
                    String [] arr = s.split(":");
                    log.info("server ip: "+arr[0]+" "+inaddr);
                    if(inaddr.equals(arr[0])){
                        inaddr+=":"+arr[2];
                        flag = true;
                        break;
                    }
                }
                if(!flag){
                    inaddr+=":"+sk.getPort();
                }

                Handler handler = new Handler(sk, this,inaddr);

                socketThreads.put(inaddr, handler);
                log.info("socketmanager put address: "+inaddr);
                handlers.put(handler,0);
                Thread t = new Thread(handler);
                t.start();
            } catch (IOException e) {
                e.printStackTrace();

            }
            // accept 之后分给其他线程，单个线程先接收信息，根据信息来判断是region还是client，然后调用不同的方法进行处理
        }

    }

    public void removeSocket(String addr) {
        synchronized (this){
            Handler h = socketThreads.get(addr);
            handlers.remove(h);
            socketThreads.remove(h);
        }

    }

    public void printSockets(){
        for(Map.Entry<String,Handler>e:socketThreads.entrySet()){
            System.out.println(e.getKey()+" "+e.getValue());
        }
    }

    public Handler getHandler(String addr) {
        synchronized (this){
            log.info("gethandler:"+tableManager.serverCount());
            printSockets();
            String new_addr = addr;
            String [] str = addr.split(":");
            if(str.length>2){
                new_addr = str[0]+":"+str[2];
            }

//            Set<String> set = tableManager.serverTables.keySet();
            return socketThreads.get(new_addr);
        }

    }


    }
