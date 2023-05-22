import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BlockingQueue;
@Slf4j
public class WriteHandler implements Runnable{


    private  ZKManager zkManager;

    private BlockingQueue<WriteTask> writeTasks;

    public WriteHandler(BlockingQueue<WriteTask>writeTasks,ZKManager zkManager){
        this.zkManager = zkManager;
        this.writeTasks = writeTasks;

    }

    private String  buildPath(String tbName,String sqlcmd){
        try {
            String path = "/" + tbName + "/" + "trx";
            path = zkManager.addNodeForPath(path, Mode.SEQUENTIAL,null);
//            zkManager.client.inTransaction().create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).
//                    forPath(path).and().create().withMode(CreateMode.EPHEMERAL).forPath(path+"/"+"sql",sqlcmd.getBytes() ).and().create().withMode(CreateMode.EPHEMERAL).
//                    forPath(path + "/" + "resultPrepare").and().create().withMode(CreateMode.PERSISTENT).
//                    forPath(path + "/" + "resultCommit").and().create().withMode(CreateMode.EPHEMERAL).
//                    forPath(path + "/" + "commit").and().create().withMode(CreateMode.EPHEMERAL).
//                    forPath(path + "/" + "rollback").and().create().withMode(CreateMode.EPHEMERAL).
//                    forPath(path + "/" + "binlog");
            zkManager.addNodeForPath(path+"/"+"sql",Mode.TEMPORARILY,sqlcmd);
            zkManager.addNodeForPath(path + "/" + "resultPrepare",Mode.PERMANENT,null);
            zkManager.addNodeForPath(path + "/" + "resultCommit",Mode.PERMANENT,null);
            zkManager.addNodeForPath(path + "/" + "commit",Mode.PERMANENT,null);
            zkManager.addNodeForPath(path +"/" + "rollback" ,Mode.PERMANENT,null);
            zkManager.addNodeForPath(path + "/" + "binlog",Mode.PERMANENT,null);
            return path;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
    @Override
    public void run(){

            try{
                while (true) {
                    log.info("running");
                    WriteTask writeTask = writeTasks.take();
                    log.info("take task");
                    String path = buildPath(writeTask.getTbName(), writeTask.getSql());

                    zkManager.ListenWrite(writeTask.getId(), writeTask.getIP(), path, writeTask.getTbName(), writeTask.IsCreate(),writeTask.IsDrop(),
                            writeTask.getSql());
                }
                }catch (InterruptedException e){
                e.printStackTrace();
            }



       //创建node通知server

    }
}
