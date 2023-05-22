package config;

public class Config {
    /**
     * region的配置信息
     */
    public static final String IP = "192.168.43.222"; // region的IP
    public static final int REGION_PORT = 8082; // 和region通信的端口
    public static final int CLIENT_PORT = 4321; // 和客户端通信的端口
    public static final String MASTER_IP = "192.168.43.83"; // master的IP
    public static final int MASTER_PORT = 1234; // 和master通信的端口

    /**
     * zookeeper的配置信息
     */
    public static final int ZK_CLIENT_PORT = 2183; // 作为zookeeper client的端口
    public static final String ZK_ADDR = IP + ":" + ZK_CLIENT_PORT;
    public static final String PATH = "/region/node4";
}
