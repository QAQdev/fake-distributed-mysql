import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class PathChangeListener {
    static class AnotherListener {
        public void processNodeChange(ChildData data) {
            // 在这里处理节点变化通知
            System.out.println("Processing node change: " + data.getPath());
        }
    }
    private static final String ZK_CONNECTION_STRING = "172.20.10.2:2182";
    private static final String ZK_PATH = "/path";
    static AnotherListener anotherListener = new AnotherListener();
    public static void main(String[] args) throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(ZK_CONNECTION_STRING, new ExponentialBackoffRetry(1000, 3));
        client.start();

        // 创建第一个监听器，监听节点变化
        PathChildrenCache cache = new PathChildrenCache(client, ZK_PATH, true);
        cache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                if (event.getType() == PathChildrenCacheEvent.Type.CHILD_UPDATED) {
                    ChildData data = event.getData();
                    System.out.println("Node changed: " + data.getPath());

                    // 调用第二个监听器进行处理
                    anotherListener.processNodeChange(data);
                }
            }
        });
        cache.start();

        // 创建第二个监听器，处理节点变化


        // 继续其他操作...

        // 关闭Curator客户端
        cache.close();
        client.close();
    }


}
