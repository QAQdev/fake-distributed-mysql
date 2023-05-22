import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Set;

/**
 * @Description TODO
 **/
@Slf4j
public class Handler implements Runnable {

    private Socket clientSocket;
    // 所有handler共用一个clientProcessor,拥有私有的regionProcessor
    private ClientProcessor clientProcessor;
    private RegionProcessor regionProcessor;

    public BufferedReader input = null;
    public PrintWriter output = null;
    protected SocketManager socketManager;

    protected TableManager tableManager;

    private boolean isClose;

    String addr; // socketManager传入的ip:port
    // 一个用来处理region 信息，一个用来处理客户端信息
    public Handler(Socket clientSocket, SocketManager socketManager,String addr) throws IOException {
        System.out.println("Handler");
        this.clientSocket = clientSocket;
        this.input = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        this.output = new PrintWriter(clientSocket.getOutputStream());
        this.socketManager = socketManager;
        this.tableManager = socketManager.tableManager;

        isClose = false;

        this.clientProcessor = socketManager.clientProcessor;
        this.regionProcessor = new RegionProcessor(this);
        this.addr = addr;
    }

    /**
     * 发送消息给连接的socket，由于输出流有可能在其他地方使用，需要使用synchronized保护
     *
     * @param data
     */
    public void sendMessage(String data) {

        synchronized (this) {
            log.info("send message: "+data+" "+isClose+" "+clientSocket.getInetAddress().toString());

            if(!isClose){
                this.output.println(data);
                this.output.flush();
            }

        }

    }

    /**
     * 获取region server的ip，包括：ip:port1:port2
     * @return
     */
    public String getRegionFullIP() {
        Set<String> servers = null;
        synchronized (tableManager){
            servers = tableManager.serverTables.keySet();
        }

        String [] this_ip = addr.split(":");
        for(String s:servers){
            // 如果匹配了ip，看看port是否匹配
            if(s.contains(this_ip[0])){
                String [] server_ip = s.split(":");
                if(server_ip[2].equals(this_ip[1])){
                    return s;

                }

            }
        }
        return null;

    }

    @Override
    public void run() {
        String line;
        try {
            while (true) {

                String result = "";
                line = input.readLine();

                //处理指令并获取返回的结果，注意返回的result也可能为空
                if (line != null) {
                    log.info("handler收到消息:"+line);
                    if (line.startsWith("<client-master>")) {
                        //client会自己写回
                        clientProcessor.process(clientSocket.getInetAddress().getHostAddress()+":"+clientSocket.getPort(),line.substring(15));
                    } else if (line.startsWith("<region-master>")) {

                        result = regionProcessor.process(line.substring(15));
                    }
                }
                if (!result.equals("")) {
                    sendMessage(result);
                }


            }

        } catch (Exception e) {
            // 客户/region断开连接，更新元信息
            e.printStackTrace();
            String addr = clientSocket.getInetAddress().getHostAddress();
            log.info("与"+addr+"断开连接");
            socketManager.removeSocket(addr);
            try {
                this.input.close();
                this.output.close();
                clientSocket.close();
                isClose = true;
                //停止当前线程
            } catch (Exception exp) {
                exp.printStackTrace();
            }
        }


    }
}
