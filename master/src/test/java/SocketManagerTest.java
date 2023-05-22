import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * @Description TODO
 **/
public class SocketManagerTest {

    public static void main(String[] args){
        SocketAddress addr = new InetSocketAddress("127.0.0.1",5678);
            new Thread(()->{
                try{
                    SocketManager sk = new SocketManager(addr,null,null,null);
                    sk.startService();

                }catch(Exception e){
                    e.printStackTrace();
                }
            }).start();


    }
}
