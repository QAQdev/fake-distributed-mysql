import lombok.extern.slf4j.Slf4j;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
@Slf4j
class WriteTask{
    private String ip;
    private String id;
    private String sql;
    private String tbName;
    private boolean isCreate;
    private boolean isDrop;
    public boolean IsDrop(){return isDrop;}

    public String getId(){
        return id;
    }
    public String getIP(){
        return ip;
    }


    public String getSql(){
        return sql;
    }

    public String getTbName() {
        return tbName;
    }
    public boolean IsCreate(){
        return isCreate;
    }

    public WriteTask(String ip,String tbName, String sql, boolean isCreate,boolean isDrop){
        this.id = UUID.randomUUID().toString();
        this.ip = ip;
        this.sql = sql;
        this.tbName = tbName;
        this.isCreate = isCreate;
        this.isDrop  = isDrop;

    }
}
class WriteResult{
    private String id;
    private String ip;
    private String tbName;
    private boolean isCreate;
    private boolean isSuccess;


    public WriteResult(String id,String ip,String tbName,boolean isCreate,boolean isSuccess){
        this.id = id;
        this.ip = ip;
        this.tbName = tbName;
        this.isCreate = isCreate;
        this.isSuccess = isSuccess;

    }
    public String getIP(){
        return ip;
    }
    public String getTbName(){
        return tbName;
    }

    public boolean IsSuccess() {
        return isSuccess;
    }

    public boolean IsCreate() {
        return isCreate;
    }
}
@Slf4j
class TaskHandle implements Runnable{
    private WriteTask writeTask;
    private BlockingQueue<WriteTask> writeTasks;
    public TaskHandle(BlockingQueue<WriteTask>writeTasks,WriteTask writeTask){
        this.writeTask = writeTask;
        this.writeTasks = writeTasks;

    }
    public void run(){
        try {
            log.info("put task");
            writeTasks.put(writeTask);
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
@Slf4j
class ResultHandle implements  Runnable{
    public boolean exit= false;

    private BlockingQueue<WriteResult> writeResults;
    public String returnHeader;
    private TableManager tableManager;
    private SocketManager socketManager;
    public ResultHandle(BlockingQueue<WriteResult>wr, String rh,TableManager tableManager,SocketManager socketManager){
        this.writeResults = wr;
        this.returnHeader = rh;
        this.tableManager = tableManager;
        this.socketManager = socketManager;

    }
    public void run(){
        try {
            while(true){

                WriteResult wr =writeResults.take();
                String result ;
                if(wr.IsCreate()){
                    result = returnHeader + "[2] " + wr.getTbName() +" "+tableManager.TableServersToString(wr.getTbName(),false);
                    //region ip
                }else{
                    result = returnHeader +"[3] " + wr.IsSuccess();
                }
                synchronized (socketManager){
                    log.info("result handle: "+wr.getIP()+" "+socketManager.socketThreads.containsKey(wr.getIP()));
                    if(socketManager.socketThreads.containsKey(wr.getIP())){

                        socketManager.getHandler(wr.getIP()).sendMessage(result);
                    }

                }

            }
        }catch (InterruptedException e) {
                throw new RuntimeException(e);
            }


    }
}
/**
 * 1.把任务提交给BlockingQueue
 **/
@Slf4j
public class ClientProcessor {
    public SocketManager socketManager;

    private BlockingQueue<WriteTask> writeTasks ;
    private BlockingQueue<WriteResult> writeResults ;
    private String returnHeader = "<master-client>";

    private ResultHandle resultHandle;
    private TableManager tableManager;
    private ExecutorService executor;


    public ClientProcessor(BlockingQueue<WriteTask>wt, BlockingQueue<WriteResult> wr,TableManager tableManager,SocketManager socketManager) {
        this.writeTasks = wt;
        this.writeResults = wr;
//        this.socketManager = socketManager;
        this.tableManager = tableManager;
        this.executor = Executors.newFixedThreadPool(4);


    }
    public void init(SocketManager sk){
        this.socketManager = sk;
        this.resultHandle = new ResultHandle(writeResults,returnHeader,tableManager,socketManager);
        new Thread(this.resultHandle).start();

    }
    private String getTableNameFromCMD(String cmd){

        StringBuilder sb = new StringBuilder();
        char[] ca = cmd.toCharArray();
        for(int i = 1; i< ca.length;i++){
            if(ca[i] ==')'){
                break;
            }else{
                sb.append(ca[i]);
            }
        }
        return  sb.toString();
    }
    private String getSqlFromCMD(String tbName,String cmd){
        int len = tbName.length();
        String sql = cmd.substring(len +2);
        return sql;
    }

    /**
     * new1个线程写

     * @param cmd
     */

    public void process(String ip,String cmd) {
        WriteTask wr;
        if(cmd.startsWith("[2]")){
            cmd = cmd.substring(3);
            //

            String tbName = getTableNameFromCMD(cmd);
            String sql = getSqlFromCMD(tbName,cmd);
            wr = new WriteTask(ip,tbName,sql,   true,false);
            TaskHandle taskHandle1 = new TaskHandle(writeTasks,wr);
            executor.execute(taskHandle1);
        }else if(cmd.startsWith("[3]")){
            //给region发消息
            cmd = cmd.substring(3);
            String tbName = getTableNameFromCMD(cmd);
            String sql = getSqlFromCMD(tbName,cmd);

            if(!tableManager.CheckTableExits(tbName)){
                log.info("clientprocessor: table not exits! "+tbName);
                tableManager.printTableManager();
                synchronized ( socketManager.socketThreads){
                    if(socketManager.getHandler(ip)!=null){
                        socketManager.getHandler(ip).sendMessage("<master-client>[3]" + "0");
                    }
                }
            }else{
                log.info("clientprocessor: table exits! "+tbName);
                tableManager.printTableManager();

                if(sql.startsWith("drop")){
                    wr = new WriteTask(ip,tbName,sql,false,true);
                }else{
                    wr = new WriteTask(ip,tbName,sql,   false,false);
                }


                TaskHandle taskHandle1 = new TaskHandle(writeTasks,wr);
                executor.execute(taskHandle1);
            }

        }else if(cmd.startsWith("[1]")){
            cmd = cmd.substring(3);
            //直接返回表名
            String tbName = getTableNameFromCMD(cmd);
            String ips = tableManager.TableServersToString(tbName,true);
            synchronized ( socketManager.socketThreads){
                if(socketManager.getHandler(ip)!=null){
                    if(ips.equals("")){
                        socketManager.getHandler(ip).sendMessage("<master-client>[1] " + tbName + " " +"table not exits");

                    }else{
                        socketManager.getHandler(ip).sendMessage("<master-client>[1] " + tbName + " " +ips);

                    }
                }
            }


        }

    }

//    接受处理消息结果

}
