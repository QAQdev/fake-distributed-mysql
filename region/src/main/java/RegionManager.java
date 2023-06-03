import config.Config;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.sql.Connection;

public class RegionManager implements Runnable {
    private Logger logger;
    private DatabaseManager databaseManager;
    private Connection connection;
    private MasterSocketManager masterSocketManager;
    private ClientSocketManager clientSocketManager;
    private RegionSocketManager regionSocketManager;
    private RegionZooKeeper regionZooKeeper;


    public RegionManager() throws Exception {
        logger = Logger.getLogger(DatabaseManager.class);

        // database connection via DatabaseManager
        try {
            databaseManager = new DatabaseManager();
            connection = databaseManager.connection;
            logger.log(Level.INFO, "MySQL connection established: \n" + databaseManager.getMetaInfo() + "\n" + connection.getClientInfo());
        } catch (Exception e) {
            logger.log(Level.INFO, "MySQL connection error: " + e);
        }

        // region connection via RegionSocketManager
        try {
            regionSocketManager = new RegionSocketManager(Config.REGION_PORT, databaseManager);
            logger.log(Level.INFO, "Region socket connection prepared: \n");
        } catch (Exception e) {
            logger.log(Level.INFO, "Region socket connection failed: " + e);
        }

        try {
            regionZooKeeper = new RegionZooKeeper();
            logger.log(Level.INFO, "Zookeeper connection prepared.");
            Thread regionZookeeperThread = new Thread(regionZooKeeper);
            regionZookeeperThread.start();
        } catch (Exception e) {
            logger.log(Level.INFO, "Zookeeper connection failed: " + e);
        }

        Thread.sleep(5 * 1000);

        // master connection via MasterSocketManager
        try {
            masterSocketManager = new MasterSocketManager(Config.MASTER_IP, Config.MASTER_PORT, regionSocketManager, databaseManager);
            logger.log(Level.INFO, "Master socket connection prepared: \n");
        } catch (Exception e) {
            logger.log(Level.INFO, "Master socket connection failed: " + e);
        }

        // client connection via MasterSocketManager
        try {
            clientSocketManager = new ClientSocketManager(Config.CLIENT_PORT, databaseManager);
            logger.log(Level.INFO, "Client socket connection prepared: \n");
        } catch (Exception e) {
            logger.log(Level.INFO, "Client socket connection failed: " + e);
        }
    }

    public Connection getConnection() {
        return connection;
    }

    public void run() {
        try {

            logger.log(Level.INFO, "RegionZookeeper Thread started");

            Thread masterSocketThread = new Thread(masterSocketManager);
            masterSocketThread.start();

            logger.log(Level.INFO, "Master Thread started");


            Thread regionSocketThread = new Thread(regionSocketManager);
            regionSocketThread.start();

            logger.log(Level.INFO, "Region Thread started");

            Thread clientSocketTHREAD = new Thread(clientSocketManager);
            clientSocketTHREAD.start();

            logger.log(Level.INFO, "Client Thread started");
        } catch (Exception e) {
            logger.log(Level.INFO, "Thread starting failed: " + e);
        }
    }
}
