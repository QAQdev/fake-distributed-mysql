import config.Config;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.*;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

enum Mode {
    PERMANENT, TEMPORARILY, SEQUENTIAL  //permanent  temporarily
}

public class RegionZooKeeper implements Runnable {
    private Logger logger;
    private CuratorFramework client;

    public RegionZooKeeper() {
        logger = Logger.getLogger(RegionZooKeeper.class);

        try {
            // 连接到集群
            client = CuratorFrameworkFactory.builder().connectString(Config.ZK_ADDR).sessionTimeoutMs(5000).
                    retryPolicy(new ExponentialBackoffRetry(1000, 3)).
                    build();

            client.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    void register() throws InterruptedException, KeeperException, IOException {
        try {
            // Connect to the ZooKeeper server as a client
            ZooKeeper zooKeeper = new ZooKeeper("localhost:" + Config.ZK_CLIENT_PORT, 3000, null);

            // Define the znode path and data
//            String data = "192.168.43.222:4321:1234:8082";
            String data = Config.IP + ":" + Config.CLIENT_PORT + ":" + Config.MASTER_PORT + ":" + Config.REGION_PORT;

            // Create the znode on the server
            addNodeForPath(Config.PATH, Mode.PERMANENT, data);
            logger.info(new String(client.getData().forPath(Config.PATH)));

            synchronized (this) {
                wait();
            }
            // Close the connection to the ZooKeeper server
            zooKeeper.close();
        } catch (InterruptedException | KeeperException | IOException e1) {
            logger.log(Level.INFO, e1);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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

    @Override
    public void run() {
        try {
            this.register();
        } catch (InterruptedException | KeeperException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}