import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import utils.CacheUtil;
import utils.CommandHeader;
import utils.StatisticsUtil;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MasterSocketManager implements Runnable {
    private Socket socket;
    private BufferedReader input;
    private PrintWriter output;
    private Logger logger;
    private boolean isRunning;
    private RegionSocketManager regionSocketManager;
    private DatabaseManager databaseManager;
    private CuratorFramework client;
    Map<String, PathChildrenCache> cacheList;


    public MasterSocketManager(String ip, int port, RegionSocketManager regionSocketManager, DatabaseManager databaseManager) throws Exception {
        this.regionSocketManager = regionSocketManager;
        this.databaseManager = databaseManager;
        this.socket = new Socket(ip, port);
        this.input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.output = new PrintWriter(socket.getOutputStream(), true);
        this.isRunning = true;
        this.logger = Logger.getLogger(MasterSocketManager.class);
        this.client = CacheUtil.getInstance().client;
        this.cacheList = CacheUtil.getInstance().cacheList;

//        // 监听已有表
//        String[] existingTables = databaseManager.getMetaInfo().split(" ");
//        for (String table : existingTables) {
//            cacheList.put(table, new PathChildrenCache(client, "/" + table, true));
//            cacheList.get(table).start();
//            addListener("/" + table, cacheList.get(table));
//        }
        loadBalanceStatistics(2);
    }

    private String addNodeForPath(String path, Mode m, String value) throws Exception {
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
                cm = CreateMode.EPHEMERAL_SEQUENTIAL;
                break;
            }
        }
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
    }

    private void addListener(String path, PathChildrenCache cache) {
        PathChildrenCacheListener listener = (client, event) -> {
            logger.info("listener added at " + event.getData().getPath());
//            CountDownLatch latch = new CountDownLatch(1);
            switch (event.getType()) {
                // 节点增加，即trx_id增加
                case INITIALIZED:
                case CHILD_UPDATED:
                case CHILD_ADDED: {
                    String trx_id = ZKPaths.getNodeFromPath(event.getData().getPath());
                    // 获取/path/sql的sql语句
                    Thread.sleep(500);
                    String sql = new String(client.getData().forPath(event.getData().getPath() + "/sql"));
                    logger.info("SQL " + sql + " received.");
                    PathChildrenCache commitCache = new PathChildrenCache(client, event.getData().getPath() + "/commit", true);
                    PathChildrenCache rollbackCache = new PathChildrenCache(client, event.getData().getPath() + "/rollback", true);

                    // 执行XA PREPARE，将XA PREPARE写到path/resultPrepare中
                    try {

                        // 获取XAConnection对象
//                        XAConnection xaConn = databaseManager.xaConnection;
//                        Connection conn = xaConn.getConnection();
//                        XAResource xaRes = xaConn.getXAResource();
//
//                        // 创建Xid对象
//                        Xid xid = new MysqlXid(trx_id.getBytes(StandardCharsets.UTF_8),
//                                trx_id.getBytes(StandardCharsets.UTF_8), 1);
//
//                        // 开始XA事务
//                        xaRes.start(xid, XAResource.TMNOFLAGS);
//
//                        // 执行SQL语句
                        Statement stmt = databaseManager.connection.createStatement();
                        stmt.executeUpdate(sql);
//
//                        // 执行XA Prepare操作
//                        xaRes.end(xid, XAResource.TMSUCCESS);
//                        int ret = xaRes.prepare(xid);
                        int ret = 1;
                        logger.info("SQL " + sql + " prepared.");

                        // 将XA PREPARE结果写到path/resultPrepare中
//                        String address = InternetUtil.getLocalHostExactAddress().toString() + ":" + ZK_PORT;
                        String address = "192.168.43.222:4321:1234";
                        String result = String.valueOf(ret);
                        addNodeForPath(event.getData().getPath() + "/resultPrepare/" + address, Mode.PERMANENT, result);
                        addNodeForPath(event.getData().getPath() + "/resultCommit/" + address, Mode.PERMANENT, result);

                        String tableName = databaseManager.getTableName(sql);
                        StatisticsUtil.getInstance().update(tableName);

                        ArrayList<String> existingTables = new ArrayList<>(Arrays.asList(databaseManager.getMetaInfo().split(" ")));
                        for (String table : cacheList.keySet()) {
                            if (!existingTables.contains(table)) {
                                cacheList.get(table).close();
                                cacheList.remove(table);
                            }
                        }

//                        client.setData().forPath(event.getData().getPath() + "/resultPrepare" + address, result.getBytes());

//                        commitCache.getListenable().addListener((client1, event1) -> {
//                            switch (event1.getType()) {
//                                case CHILD_UPDATED:
//                                case CHILD_ADDED: {
//                                    String data = client1.getData().forPath(event1.getData().getPath()).toString();
//                                    // 如果是commit，则写入resultCommit
//                                    if (data.equals("1")) {
//                                        try {
////                                            xaRes.commit(xid, false);
//                                            addNodeForPath(event.getData().getPath() + "/resultCommit" + address, Mode.PERMANENT, "1");
////                                            client.setData().forPath(event1.getData().getPath() + "resultCommit" + address, "1".getBytes());
//                                            logger.info(sql + "committed.");
//                                            latch.countDown();
//                                        } catch (Exception e) {
//                                            logger.info(e);
//                                            addNodeForPath(event.getData().getPath() + "/resultCommit" + address, Mode.PERMANENT, "0");
////                                            client.setData().forPath(event1.getData().getPath() + "resultCommit" + address, "0".getBytes());
//                                            logger.info(sql + "commit failed.");
//                                            latch.countDown();
//                                        }
//                                    }
//                                    break;
//                                }
//                                default: {
//                                    break;
//                                }
//                            }
//                        });
//
//                        rollbackCache.getListenable().addListener((client1, event1) -> {
//                            switch (event1.getType()) {
//                                case CHILD_UPDATED:
//                                case CHILD_ADDED: {
//                                    String data = client1.getData().forPath(event1.getData().getPath()).toString();
//                                    // 如果是rollback，做rollback操作
//                                    if (data.equals("1")) {
//                                        try {
////                                            xaRes.rollback(xid);
//                                            logger.info("rollback succeeded");
//                                            latch.countDown();
//                                        } catch (Exception e) {
//                                            logger.info(e);
//                                            latch.countDown();
//                                        }
//                                    }
//                                    break;
//                                }
//                                default: {
//                                    break;
//                                }
//                            }
//                        });
//
//                        commitCache.start();
//                        rollbackCache.start();

//                        latch.await();

                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        // 关闭cache
                        commitCache.close();
                        rollbackCache.close();
                    }

                    break;
                }
                default: {
                    break;
                }
            }
        };
        cache.getListenable().addListener(listener);
    }

    private void execute(String cmd) throws Exception {
        logger.log(Level.INFO, "收到 Master 命令：" + cmd);
        // 拷贝副本，容错容灾
        if (cmd.startsWith(CommandHeader.MASTER_TO_REGION_1.value)) {
            // 调 Region 的方法
            String commandBody = cmd.substring(CommandHeader.MASTER_TO_REGION_1.value.length());
            output.println(regionSocketManager.initiatingTableCopying(commandBody) + cmd);
        }
        // 负载均衡：迁移表格
        else if (cmd.startsWith(CommandHeader.MASTER_TO_REGION_3.value)) {
            // 调 Region 的方法
            String commandBody = cmd.substring(CommandHeader.MASTER_TO_REGION_3.value.length());
            output.println(regionSocketManager.initiatingTableTransferring(commandBody) + cmd);
        }
        // 写同步
        else {
            String commandBody = cmd.substring(CommandHeader.MASTER_TO_REGION_2.value.length());
            String tableName = databaseManager.getTableName(commandBody);
            Pattern pattern = Pattern.compile("(DROP|CREATE)", Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(commandBody);
            while (matcher.find()) {
                // 处理create
                if (matcher.group().toUpperCase().equals("CREATE")) {
//                    Statement statement = databaseManager.connection.createStatement();
//                    statement.executeUpdate(commandBody);
//                    statement.close();
                    String[] existingTables = (databaseManager.getMetaInfo() + "" + tableName).split(" ");
                    for (String table : existingTables) {
                        if (table.equals(tableName) && !cacheList.containsKey(tableName)) {
                            cacheList.put(table, new PathChildrenCache(client, "/" + table, true));
                            cacheList.get(table).start();
                            addListener("/" + table, cacheList.get(table));
                        }
                    }
                }
                // 处理drop
                else if (matcher.group().toUpperCase().equals("DROP")) {
                    if (cacheList.containsKey(tableName)) {
//                        Statement statement = databaseManager.connection.createStatement();
//                        statement.executeUpdate(commandBody);
//                        statement.close();
//                        cacheList.get(tableName).close();
//                        cacheList.remove(tableName);
                    }
                }
            }
        }
    }

    private void loadBalanceStatistics(int k) {
        Thread checker = new Thread(() -> {
            while (isRunning) {
                try {
                    String check = CommandHeader.REGION_TO_MASTER_2.value + StatisticsUtil.getInstance().getTopK(k);
                    logger.log(Level.INFO, check);
                    output.println(check);
                    StatisticsUtil.getInstance().clear();
                    logger.log(Level.INFO, "30s 过期数据清理");
                    Thread.sleep(1000 * 30);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        checker.start();
    }

    @Override
    public void run() {
        try {
            Thread.sleep(1000 * 10);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        logger.log(Level.INFO, "Region 监听 Master 消息中..." + socket.getInetAddress() + ":" + socket.getPort());
        while (isRunning) {
            try {
                Thread.sleep(1000);
                logger.log(Level.INFO, "here1");
                String line = input.readLine();
                logger.log(Level.INFO, "here2");
                if (line != null) {
                    execute(line);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
