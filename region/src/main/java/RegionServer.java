
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class RegionServer {
    private static Logger logger;
    public static void main(String[] args) throws Exception {
        logger = Logger.getLogger(RegionServer.class);
        try {

            RegionManager regionManager = new RegionManager();
            regionManager.run();
            logger.log(Level.INFO, "RegionServer Running.");
        }
        catch (Exception e) {
            logger.log(Level.INFO, "RegionServer Failed.");
        }
    }
}
