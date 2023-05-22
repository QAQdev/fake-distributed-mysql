package utils;

import config.Config;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

public class CacheUtil {


    private static CacheUtil cacheHandle;
    private Logger logger;
    public CuratorFramework client;

//    public String ZK_PORT = "2183";
//    public String ZK_ADDR = "192.168.43.222:" + ZK_PORT;
    public Map<String, PathChildrenCache> cacheList;

    // 表名：访问次数
    private static Map<String, Integer> statisticsMap = new LinkedHashMap<>();

    private CacheUtil() {
        this.logger = org.apache.log4j.Logger.getLogger(CacheUtil.class);
        client = CuratorFrameworkFactory.builder().connectString(Config.ZK_ADDR).sessionTimeoutMs(5000).
                retryPolicy(new ExponentialBackoffRetry(1000, 3)).
                build();
        client.start();
        cacheList = new HashMap<>();
    }

    public static synchronized CacheUtil getInstance() {
        return Objects.requireNonNullElseGet(cacheHandle, CacheUtil::new);
    }

    public void cancelListenTable(String dbMeta) {
        ArrayList<String> existingTables = new ArrayList<>(Arrays.asList(dbMeta.split(" ")));
        for (String table : cacheList.keySet()) {
            if (!existingTables.contains(table)) {
                try {
                    cacheList.get(table).close();
                    cacheList.remove(table);
                    logger.info(table + " removed.");
                } catch (IOException e) {
                    logger.info(table + " removed failed.");
                }
            }
        }
    }

    public void listenNewTable(String tableName, String dbMeta) {
        ArrayList<String> existingTables = new ArrayList<>(Arrays.asList(dbMeta.split(" ")));
        if (existingTables.contains(tableName) && !cacheList.containsKey(tableName)) {
            cacheList.put(tableName, new PathChildrenCache(client, "/" + tableName, true));
            try {
                cacheList.get(tableName).start();
                logger.info(tableName + " listening.");
            } catch (Exception e) {
                logger.info(tableName + " listening failed: " + e);
            }
        }
    }
}
