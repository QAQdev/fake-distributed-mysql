import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

/**
/**
 * @Description TODO
 **/
public class ZKManagerTest {
    public String addNodeForPath(String path, Mode m, String value) throws Exception {
        CreateMode cm = null;
        switch (m) {
            case PERMANENT: {
                cm = CreateMode.PERSISTENT;
            }
            case TEMPORARILY: {
                cm = CreateMode.EPHEMERAL;
            }
            case SEQUENTIAL: {
                cm = CreateMode.EPHEMERAL_SEQUENTIAL;
            }
        }
        if (client.checkExists().forPath(path) == null) {
            String nodePath;
            if (value != null) {
                System.out.println("here");
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
    private CuratorFramework client;
        public static String PATH = "/regions";

        public static void main(String[] args) throws Exception
        {
            ZKManagerTest zk = new ZKManagerTest();




        }
        public ZKManagerTest(){
            RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);

            client = CuratorFrameworkFactory.newClient("172.20.10.2:2182", retryPolicy);

            client.start();
            try {
                String path = "/a234";
                System.out.println("here");
                String a =addNodeForPath(path,Mode.TEMPORARILY,"hhh");

                System.out.println("hereeee");
                byte[] b= client.getData().forPath(a);
                String result = new String(b);
                System.out.println("result"+result);
                //List<String> ss= client.getChildren().forPath("/region");
                //
                //for(String s:ss){
                //    System.out.println(s);
                //}
                //System.out.println(a);
            }catch (Exception e){

            }
        }


}
