import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;

import java.util.*;
import java.util.concurrent.BlockingQueue;



enum Mode {
    PERMANENT, TEMPORARILY, SEQUENTIAL  //permanent  temporarily
}
@Slf4j
public class ZKManager  {
    final private TableManager tableManager;
    final private SocketManager socketManager;
    final private static String ZK_ADDR = "172.20.10.7:2184";
    final private static String PATH = "/regions";
    CuratorFramework client = null;
    PathChildrenCache cache = null;
    final private BlockingQueue<WriteResult> writeResults;

    public ZKManager(TableManager t, SocketManager socketManager, BlockingQueue<WriteResult> wr) {
        this.writeResults = wr;
        this.tableManager = t;
        this.socketManager = socketManager;
    }



    public void startZookeeper() {
        try {
            // 连接到集群
            client = CuratorFrameworkFactory.builder().connectString(ZK_ADDR).sessionTimeoutMs(5000).
                    retryPolicy(new ExponentialBackoffRetry(1000, 3)).
                    build();

            client.start();

            cache = new PathChildrenCache(client, PATH, true);

            cache.start();
            // 监听
            addListener(cache);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //CloseableUtils.closeQuietly(cache);
            //CloseableUtils.closeQuietly(client);
        }
    }

    /**
     * 把结果写入writeResults
     *
     * @param id id
     * @param tbName tablename
     * @param isCreate boolean
     * @param isSuccess boolean
     *
     */
    public void addResult(String id, String ip, String tbName, boolean isCreate, boolean isSuccess) {
        WriteResult wr = new WriteResult(id, ip,tbName, isCreate, isSuccess);
        writeResults.add(wr);
    }
    //

    public String addNodeForPath(String path, Mode m, String value)  {
        CreateMode cm = null;
        switch (m) {
            case PERMANENT: {
                cm = CreateMode.PERSISTENT;
                break;
            }
            case TEMPORARILY: {
                cm = CreateMode.EPHEMERAL;
                break;
            }
            case SEQUENTIAL: {
                cm = CreateMode.PERSISTENT_SEQUENTIAL;
                break;
            }
        }
        try{
        if (client.checkExists().forPath(path) == null) {
            String nodePath;
            if (value != null) {
                nodePath = client.create().creatingParentsIfNeeded().
                        withMode(cm).forPath(path, value.getBytes());
            } else {
                nodePath = client.create().creatingParentsIfNeeded().
                        withMode(cm).forPath(path);
            }

            return nodePath;
        } else {
            return null;
        }
        }catch (Exception e)
        {
            e.printStackTrace();
            log.error("add node for path: "+path+" failed");
            return null;
        }
    }

    private void HandleCommitFailed(String path, String ip, String id, String tbName, boolean isCreate, PathChildrenCache commitCache) throws Exception {
        log.info("handle commit faild: "+tbName+" "+ip);
        addNodeForPath(path+"/binlog/1",Mode.TEMPORARILY,"1");
        //client.setData().forPath(path + "/" + "binlog", "1".getBytes());
        addResult(id, ip, tbName, isCreate, false);
        deleteNodefromPath(path);

        commitCache.close();
    }

    /**
     * 处理是write时，目录节点数目变化的情况
     *

     */
    private void HandleCommitSuccess(String path, String ip, String tbName, String id, boolean isCreate,boolean isDrop, PathChildrenCache commitCache, ArrayList<String> servers) throws Exception {
        //获取event子节点的值
        log.info("handle commit success!");
        if (isCreate) {
            log.info("HandleCommitSuccess"+ " add table");
            tableManager.addTable(tbName, servers);
        }else if(isDrop){
            tableManager.removeTable(tbName);
            //并且从目录里删除
                    deleteNodefromPath("/"+tbName);
        }
        addResult(id, ip, tbName, isCreate, true);
        deleteNodefromPath(path);

        commitCache.close();

    }

    private void deleteNodefromPath(String path) {
        try {
            client.delete().deletingChildrenIfNeeded().forPath(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * listen Create
     * 先选出表最少的server，在他们的目录下写入表名，然后开始普通的监听写过程，
     * 如果中间有节点失败，或者下线，直接返回失败
     * 把执行结果写入阻塞队列
     *
     * @param sql sql
     * @return
     */

    private ArrayList<String> addTableNodeforServer(String sql) {
        // 先选出表最少的一些server
        ArrayList<String> servers = tableManager.getNewTableServers();
        log.info("server size in zkmanager: "+servers.size());
        //给他们发信息

        //    给这些server写表名，通知他们 监听这个表
        for (String ad : servers) {
            try {
                Handler handler = socketManager.getHandler(ad);
                log.info("get handler, now send message");
                handler.sendMessage("<master-region>[2]"+sql);

            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        return servers;

    }
    private void NotifyDrop(String tbname,String sql){
        LinkedList<String> servers = new LinkedList<>();
        synchronized (tableManager){
           servers = tableManager.tableLocations.get(tbname);
        }

        //给他们发信息

        //    给这些server写表名，通知他们 监听这个表
        log.info("notify drop" + sql);
        for (String ad : servers) {
            try {
                Handler handler = socketManager.getHandler(ad);
                handler.sendMessage("<master-region>[2]"+sql);

            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        tableManager.printTableManager();
    }

    private void deleteTableNodeforServer(String tbName) {
        deleteNodefromPath("/"+tbName);

    }

    public void HandlePrepareSuccess(PathChildrenCache prepareCache, String path) throws Exception {
        log.info("prepare success");
        addNodeForPath(path+"/"+"commit"+"/"+"1",Mode.TEMPORARILY,"1");
        //client.setData().forPath(path + "/" + "commit", "1".getBytes());
        log.info("commit set 1");
        //把prepare的节点删掉
        deleteNodefromPath(path + "/" + "resultPrepare");
        prepareCache.close();

    }

    private void HandlePrepareFailed(String path, String ip, String id, String tbName, boolean isCreate, PathChildrenCache commitCache,  PathChildrenCache prepareCache) throws Exception {
        log.info("prepare failed");
        if (isCreate) {
            //把注册的表删掉
            deleteTableNodeforServer(tbName);
        }
        addNodeForPath(path+"/commit"+"/1",Mode.TEMPORARILY,"1");
        //client.setData().forPath(path + "/" + "rollback", "1".getBytes());
        log.info("rollback set 1");
        addResult(id, ip, tbName, isCreate, false);

        deleteNodefromPath(path);

        commitCache.close();
        prepareCache.close();
        //把这个事务node删掉
        // rollback

    }

    /**
     * listen写的同步  可以处理 写完然后挂掉的问题，但是无法处理某个结点一直没写
     * 把执行的结果写到阻塞队列里
     *
     * @param id
     * @param path
     * @param tbName

     */

    public void ListenWrite(String id, String ip, String path, String tbName, boolean isCreate,boolean isDrop,String sql) {
        log.info("path:"+path+"sql:"+sql+"path: "+path+ "tbName: "+tbName);
        final ArrayList<String> servers = isCreate ? addTableNodeforServer(sql) : null;
        if(isDrop){
            NotifyDrop(tbName,sql);
        }
        final int servercnt = isCreate ? servers.size() : 0;
        try {
            PathChildrenCache prepareCache = new PathChildrenCache(client, path + "/" + "resultPrepare", true);
            prepareCache.start();
            PathChildrenCache commitCache = new PathChildrenCache(client, path + "/" + "resultCommit", true);
            commitCache.start();
            PathChildrenCacheListener listenerCommit = new PathChildrenCacheListener() {
                @Override
                public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                    if (event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED) {
                        if (new String(event.getData().getData()).equals("0")) {
                            HandleCommitFailed(path, ip, id, tbName, isCreate, commitCache);
                        } else {
                            //新的子节点提交成功
                            int num = commitCache.getCurrentData().size();
                            if (!isCreate) {
                                if (num == tableManager.serverCountForTable(tbName)) {
                                    //所有子节点都提交成功
                                    //获取event子节点的值
                                    HandleCommitSuccess(path, ip, tbName, path, false, isDrop,commitCache, null);
                                }
                            } else {
                                if (num == servercnt) {
                                    HandleCommitSuccess(path, ip, tbName, path, true, false,commitCache, servers);
                                }
                            }
                        }

                    } else if (event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
                        int num = commitCache.getCurrentData().size();
                        if (!isCreate) {
                            if (num == tableManager.serverCountForTable(tbName)) {
//                        获取event子节点的值
                                HandleCommitSuccess(path, ip, tbName, path, false, isDrop,commitCache, servers);

                            }
                        } else {
                            //    直接回滚
                            HandleCommitFailed(path, ip, id, tbName, true, commitCache);
                        }
                    }
                }
            };
            PathChildrenCacheListener listenerPrepare = new PathChildrenCacheListener() {
                @Override
                public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                    log.info("prepare added: "+ path);
                    if (event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED) {

                        if (new String(event.getData().getData()).equals("0")) {
                            log.info("added prepare failed: "+path+" "+new String(event.getData().getData())+ " " + event.getData().getPath());
                            HandlePrepareFailed(path, ip, id, tbName, isCreate, commitCache,  prepareCache);
                        } else {
                            log.info("added prepare success: "+path+new String(event.getData().getData())+ " " +event.getData().getPath());
                            int num = prepareCache.getCurrentData().size();
                            if (!isCreate) {
                                if (num == tableManager.serverCountForTable(tbName)) {
                                    HandlePrepareSuccess(prepareCache, path);
                                }
                            } else {
                                log.info(" create");
                                if (num == servercnt) {
                                    HandlePrepareSuccess(prepareCache, path);
                                }
                            }
                        }

                    } else if (event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED) {

                        if (!isCreate) {
                            int num = prepareCache.getCurrentData().size();
                            if (num == tableManager.serverCountForTable(tbName)) {
                                //获取event子节点的值
                                HandlePrepareSuccess(prepareCache, path);
                            }
                        } else {
                            //结束
                            HandlePrepareFailed(path, ip, id, tbName, false, commitCache,  prepareCache);
                        }
                    }
                }
            };
            prepareCache.getListenable().addListener(listenerPrepare);
            commitCache.getListenable().addListener(listenerCommit);

        } catch (Exception e) {

        }


    }


    /**
     * 添加listener监听region
     *
     * @param cache
     */
    private void addListener(PathChildrenCache cache) {

        PathChildrenCacheListener listener = new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {

                switch (event.getType()) {
                    // 节点增加
                    case CHILD_ADDED: {
                        String address = new String(event.getData().getData());
                        if(address != null){
                            log.info("Zookeeper Node added: " + ZKPaths.getNodeFromPath(event.getData().getPath())+" "+address);
                            handleChildAdded(address);
                        }

                        break;
                    }
                    // 节点减少
                    case CHILD_REMOVED: {
                        String address = new String(event.getData().getData());
                        log.info("Zookeeper Node removed: " + ZKPaths.getNodeFromPath(event.getData().getPath())+" "+address);

                        handleChildRemoved(address);
                        break;
                    }
                    default: {
                        System.out.println(event.getType().toString());
                        break;
                    }
                }
            }
        };
        cache.getListenable().addListener(listener);
    }

    /**
     * 如果一个server下线了，master分配给他的任务，他从未完成，那么在对应的目录下加一个结点，表示它挂了，master别等它
     * 把它从create目录下也删除
     *
     * @param addr
     */
    private void updateServerTrx(String addr) throws Exception {

        //先从tablemanager中获得它对应的表名

        ArrayList<HashSet<String>> tables = tableManager.serverTables.get(addr);
        for (HashSet<String> hs : tables) {
            for (String t : hs) {
                //看看这个表路径下有没有未完成的写trx
                //t是表名
                String path = "/" + t;
                List<String> children = client.getChildren().forPath(path);
                if (!children.isEmpty()) {
                    //如果还有没完成的事务
                    for (String childrenPath : children) {
                        //childrenPath的格式是:/table/trix
                        List<String> cc = client.getChildren().forPath(path + "/" + childrenPath);
                        HashSet<String> ccs = new HashSet<>(cc);
                        if (ccs.contains("resultPrepare") || ccs.contains("resultCommit")) {
                            //说明还没完成prepare,那就写一个节点，结束prepare
                            addNodeForPath(path + "/" + childrenPath + "/no", Mode.TEMPORARILY, "0");
                        }
                    }

                }

            }
        }
        //如果create 目录下也有它 删掉
        //String createPath = "/create/" + addr;
        //deleteNodefromPath(createPath);
    }

    /**
     * 处理节点增加情况，更改tableManager元信息
     *
     * @param addr
     */
    public void handleChildAdded(String addr) {
        tableManager.addServer(addr);
    }

    public void handleChildRemoved(String address) {
        // 进行split，取前面的address
        String []arr = address.split(":");
        String addr = arr[0]+":"+arr[1]+":"+arr[2];
        //先把没有完成的事务提醒一下master
        try {
            updateServerTrx(addr);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        synchronized (tableManager) {
            int serverCnt = tableManager.serverCount();

            if (serverCnt == 4 || serverCnt == 2) {
            /*4个或2个slave宕机时，只需要更新tableManager元信息
                可能会出现部分数据有2个副本但部分数据只有1个副本的情况
             */
                //对于宕机节点上存储的主本，寻找对应的第一个副本机，将其置为主本服务器
                assert tableManager.serverTables.get(addr) != null;
                HashSet<String> mainTables = tableManager.serverTables.get(addr).get(0);
                for (String table : mainTables) {
                    // 删除第一个node，也即让副本机成为主本机
                    tableManager.tableLocations.get(table).remove(0);
                }
                //对于宕机节点上存储的副本，将该服务器信息删除即可
                HashSet<String> copyTables = tableManager.serverTables.get(addr).get(1);
                for (String table : copyTables) {
                    tableManager.tableLocations.get(table).remove(addr);
                }
                //从表中删除失效节点
                tableManager.removeServer(addr);

            } else if (serverCnt == 3) {
            /*3个slave的情况下发生宕机，需要进行数据迁移,注意这个时候一份数据可能有一个副本或者两个副本
              这个时候只需要把剩下的两台slave数据进行同步就可以了
            * */
                log.info("需要进行容错容灾迁移");
                // 需要迁移的全部表格数量


                // 进行数据迁移，先设置状态是migrating
                tableManager.setStatus(MasterStatus.MIGRATING);
                // 删除tableLocation中宕机节点的信息
                for (int i = 0; i <= 1; ++i) {
                    HashSet<String> tables = tableManager.serverTables.get(addr).get(i);
                    for (String table : tables) {
                        tableManager.tableLocations.get(table).remove(addr);
                    }
                }
                // 删除服务器信息
                tableManager.removeServer(addr);

                // 获取剩下的两个server ip
                //String[] servers = (String[]) tableManager.serverTables.keySet().toArray();
                //ArrayList<HashSet<String>> list = tableManager.serverTables.get(addr);

                Set<String> servers = tableManager.serverTables.keySet();
                // 获取需要迁移的所有副本数量
                Integer totalTable = 0;
                for(String table : this.tableManager.tableLocations.keySet()){
                    for(Map.Entry<String,ArrayList<HashSet<String>>> e:tableManager.serverTables.entrySet()) {
                        if(!e.getValue().get(0).contains(table)&&!e.getValue().get(1).contains(table)){
                            totalTable++;
                        }
                    }
                }
                log.info("需要迁移的所有数量为："+totalTable);
                this.tableManager.setMigrationCount(totalTable);

                //构建映射
                Map<String, String> mapping = new HashMap<>();
                for(String s: servers) {
                    String other = "";
                    for(String t:servers) {
                        if(!s.equals(t)) {
                            mapping.put(s,t);
                            break;
                        }
                    }
                }
                log.info("映射：");
                for(Map.Entry<String,String>e:mapping.entrySet()) {
                    System.out.println(e.getKey()+" "+e.getValue());
                }

                //for (int i = 0; i <= 1; ++i) {
                //    mapping.put(servers[i], servers[1 - i]);
                //}
                // 扫描表格，对于只有一个副本的表，进行数据迁移
                for (Map.Entry<String, LinkedList<String>> e : tableManager.tableLocations.entrySet()) {
                    if (e.getValue().size() == 1) {
                        String ip = e.getValue().get(0);
                        Handler handler = this.socketManager.getHandler(ip);
                        //发送<table_name>ip，命令region server将表格内容发送到ip对应的服务器处

                        handler.sendMessage("<master-region>[1]" +tableManager.getRegionPortIp(mapping.get(ip))+" "+ e.getKey());
                    }
                }

            } else {

                // 所有slave节点都宕机，系统完蛋
                log.info("System crash,all slaves died");
                System.exit(1);
            }
            // debug
            tableManager.printTableManager();
        }

    }

//    public void MultiAddNode(String tbName,String sqlcmd){
//        List<Op> ops = new ArrayList<>();
//
//        ops.add(Op.create("/example/node1", "value1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
//        ops.add(Op.create("/example/node2", "value2".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
//        resultone = client.inTransaction().create().forPath("/one", "one".getBytes()).and().check()
//                .forPath("/one").and().create().forPath("/two").and().setData().forPath("/two", "two".getBytes())
//                .and().delete().forPath("/one").and().commit();
//
//    }



}
