import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.mysql.cj.jdbc.MysqlXADataSource;
import com.mysql.cj.jdbc.MysqlXid;
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

import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import static config.Config.*;
import static javax.transaction.xa.XAResource.XA_OK;

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
            logger.info("listener event at " + event.getData().getPath());
            CountDownLatch latch = new CountDownLatch(1);
            switch (event.getType()) {
                // 节点增加，即trx_id增加
                case INITIALIZED:
                case CHILD_UPDATED:
                case CHILD_ADDED: {
                    int ret;
                    String trx_id = ZKPaths.getNodeFromPath(event.getData().getPath());
                    // 获取/path/sql的sql语句
                    Thread.sleep(500);
                    String sql = new String(client.getData().forPath(event.getData().getPath() + "/sql"));
                    logger.info("SQL " + sql + " received.");
                    Pattern pattern = Pattern.compile("(DROP TABLE|CREATE TABLE|ALTER TABLE)", Pattern.CASE_INSENSITIVE);
                    Matcher matcher = pattern.matcher(sql);
                    Statement stmt = null;

                    // 更新访问次数
                    String tableName = databaseManager.getTableName(sql);
                    StatisticsUtil.getInstance().update(tableName);

                    // 执行 DDL 语句
                    if (matcher.find()) {

                        // 执行 SQL 语句
                        try {
                            stmt = databaseManager.connection.createStatement();
                        } catch (Exception e) {
                            logger.info("statement creation failed: " + e);
                        }

                        try {
                            stmt.executeUpdate(sql);
                            stmt.close();
                            ret = 1;
                        } catch (Exception e) {
                            ret = 0;
                            logger.info("statement execution failed: " + e);
                        }

                        logger.info("DDL SQL " + sql + " execution " + ((ret == 1) ? "succeeded" : "failed"));

                        // 将结果写到 path/resultPrepare 和 path/resultCommit 中
                        String address = IP_C_M;
                        String result = String.valueOf(ret);
                        addNodeForPath(event.getData().getPath() + "/resultPrepare/" + address, Mode.PERMANENT, result);
                        addNodeForPath(event.getData().getPath() + "/resultCommit/" + address, Mode.PERMANENT, result);

                        if (sql.toUpperCase().contains("DROP TABLE")) {
                            try {
                                cacheList.get(tableName).close();
                                cacheList.remove(tableName);
                                logger.info("cancel listening table " + tableName);
                            } catch (Exception e) {
                                logger.info("failed to cancel listening " + tableName + ": " + e);
                            }
                        }


                    } else {
                        // 执行XA PREPARE，将XA PREPARE写到path/resultPrepare中
                        Connection conn = null;
                        XAConnection xaConn = null;
                        PathChildrenCache commitCache = null;
                        PathChildrenCache rollbackCache = null;
                        try {

                            commitCache = new PathChildrenCache(client, event.getData().getPath() + "/commit", true);
                            rollbackCache = new PathChildrenCache(client, event.getData().getPath() + "/rollback", true);
                            // 获取XAConnection对象
                            xaConn = databaseManager.xaConnection;
//                            conn = databaseManager.connection;
                            conn = xaConn.getConnection();
                            XAResource xaRes = xaConn.getXAResource();

                            // 创建Xid对象来表示事务
                            byte[] globalTransactionId = trx_id.getBytes();
                            byte[] branchQualifier = trx_id.getBytes();
                            Xid xid = new MysqlXid(globalTransactionId, branchQualifier, 0x01);

                            // 获取数据库连接并执行SQL语句
                            stmt = conn.createStatement();

                            // 开始XA事务
                            xaRes.start(xid, XAResource.TMNOFLAGS);

                            // 执行非 DDL 的SQL语句
                            stmt.executeUpdate(sql);

                            // 结束XA事务
                            xaRes.end(xid, XAResource.TMSUCCESS);

                            // 将XA PREPARE结果写到path/resultPrepare中
                            ret = ((xaRes.prepare(xid) == XA_OK) ? 1 : 0);
//                            ret = 1;
                            String result = String.valueOf(ret);
                            addNodeForPath(event.getData().getPath() + "/resultPrepare/" + IP_C_M, Mode.TEMPORARILY, result);
                            logger.info("SQL " + sql + " prepared.");

//                            xaRes.commit(xid, false);
//                            logger.info("SQL " + sql + " committed.");
//                            addNodeForPath(event.getData().getPath() + "/resultCommit/" + IP_C_M, Mode.TEMPORARILY, result);

                            commitCache.start();

                            commitCache.getListenable().addListener((client1, event1) -> {
                                switch (event1.getType()) {
                                    case CHILD_UPDATED:
                                    case CHILD_ADDED: {
                                        String commitPath = ZKPaths.getNodeFromPath(event1.getData().getPath());
                                        // 如果是commit，则写入resultCommit
                                        if (commitPath.equals("1")) {
                                            try {
                                                xaRes.commit(xid, false);
                                                addNodeForPath(event.getData().getPath() + "/resultCommit/" + IP_C_M, Mode.PERMANENT, "1");
//                                            client.setData().forPath(event1.getData().getPath() + "resultCommit" + address, "1".getBytes());
                                                logger.info("SQL " + sql + "committed.");
                                                latch.countDown();
                                            } catch (Exception e) {
                                                logger.info(e);
                                                addNodeForPath(event.getData().getPath() + "/resultCommit/" + IP_C_M, Mode.PERMANENT, "0");
//                                            client.setData().forPath(event1.getData().getPath() + "resultCommit" + address, "0".getBytes());
                                                logger.info("SQL " + sql + "commit failed.");
                                                latch.countDown();
                                            }
                                        }
                                        break;
                                    }
                                    default: {
                                        break;
                                    }
                                }
                            });

                            rollbackCache.start();
                            rollbackCache.getListenable().addListener((client1, event1) -> {
                                switch (event1.getType()) {
                                    case CHILD_UPDATED:
                                    case CHILD_ADDED: {
                                        String data = client1.getData().forPath(event1.getData().getPath()).toString();
                                        // 如果是rollback，做rollback操作
                                        if (data.equals("1")) {
                                            try {
                                                xaRes.rollback(xid);
                                                logger.info("SQL " + sql + " rollback succeeded.");
                                                latch.countDown();
                                            } catch (Exception e) {
                                                logger.info("SQL " + sql + " rollback failed: " + e);
                                                latch.countDown();
                                            }
                                        }
                                        break;
                                    }
                                    default: {
                                        break;
                                    }
                                }
                            });
//
//                            xaRes.commit(xid, false);
                            latch.await();
                            xaRes.end(xid, XA_OK);
                            logger.info("latch await finished.");

                        } catch (Exception e) {
                            e.printStackTrace();
                        } finally {
//                             关闭cache
                            commitCache.close();
                            rollbackCache.close();
//                             关闭连接和资源
                            assert stmt != null;
                            stmt.close();
                            conn.close();
                            xaConn.close();
                        }
                        break;
                    }
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
//            output.println(regionSocketManager.initiatingTableCopying(commandBody) + cmd);
            String info = CommandHeader.REGION_TO_MASTER_1.value +
                    regionSocketManager.initiatingTableCopying(commandBody) + " " +
                    cmd.substring(CommandHeader.MASTER_TO_REGION_1.value.length());
            output.println(info);
            logger.info(info);
        }
        // 负载均衡：迁移表格
        else if (cmd.startsWith(CommandHeader.MASTER_TO_REGION_3.value)) {
            // 调 Region 的方法
            String commandBody = cmd.substring(CommandHeader.MASTER_TO_REGION_3.value.length());
//            output.println(regionSocketManager.initiatingTableTransferring(commandBody) + cmd);
            String info = CommandHeader.REGION_TO_MASTER_3.value +
                    regionSocketManager.initiatingTableTransferring(commandBody) + " " +
                    cmd.substring(CommandHeader.MASTER_TO_REGION_3.value.length());
            output.println(info);
            logger.info(info);
        }
        // 写同步
        else {
            String commandBody = cmd.substring(CommandHeader.MASTER_TO_REGION_2.value.length());
            String tableName = databaseManager.getTableName(commandBody);
            Pattern pattern = Pattern.compile("(DROP|CREATE|ALTER)", Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(commandBody);
            while (matcher.find()) {
                // 处理create
                if (matcher.group().toUpperCase().equals("CREATE")) {
                    String[] existingTables = (databaseManager.getMetaInfo() + "" + tableName).split(" ");
                    for (String table : existingTables) {
                        if (table.equals(tableName) && !cacheList.containsKey(tableName)) {
                            cacheList.put(table, new PathChildrenCache(client, "/" + table, true));
                            cacheList.get(table).start();
                            addListener("/" + table, cacheList.get(table));
                        }
                    }
                }
                // 处理drop（无需另外处理，在transaction完成之后会自动取消对不存在表的监听）
                else if (matcher.group().toUpperCase().equals("DROP")) {

                }
                // 处理alter（无需另外处理，在transaction完成之后会自动取消对不存在表的监听）
                else if (matcher.group().toUpperCase().equals("ALTER")) {

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
//                logger.log(Level.INFO, "here1");
                String line = input.readLine();
//                logger.log(Level.INFO, "here2");
                if (line != null) {
                    execute(line);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
