package config;

import java.util.regex.Matcher;

public class Config {
    /**
     * region的配置信息
     */
    public static final String IP = "172.20.10.2"; // region的IP
    public static final String MASTER_IP = "172.20.10.7"; // master的IP
    public static final int REGION_PORT = 8082; // 和region通信的端口
    public static final int CLIENT_PORT = 4321; // 和客户端通信的端口
    public static final int MASTER_PORT = 1234; // 和master通信的端口

    /**
     * zookeeper的配置信息
     */
    public static final int ZK_CLIENT_PORT = 2182; // 作为zookeeper client的端口
    public static final String PATH = "/regions/node2";


    /**
     * 依赖于上面配置的其他信息
     */
    public static final String IP_C_M_R = IP + ":" + CLIENT_PORT + ":" + MASTER_PORT + ":" + REGION_PORT;

    public static final String IP_C_M = IP + ":" + CLIENT_PORT + ":" + MASTER_PORT;

    public static final String ZK_ADDR = IP + ":" + ZK_CLIENT_PORT;

}
