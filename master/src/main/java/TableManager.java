import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

enum MasterStatus {
    RUNNING,
    MIGRATING  //正在迁移数据，阻塞写操作
};
class Table{
    String name;
    LinkedList<String> Servers;
    int linkcnt;

}
class Server{
    String ipaddr;
    int visitCnt;
    List<HashSet<String>> tables;
    public Server(String ip,int visitCnt,List<HashSet<String>>tables){
        this.ipaddr = ip;
        this.visitCnt = visitCnt;
        this.tables = tables;
    }

}
@Slf4j

public class TableManager {
    private MasterStatus status;  //确定是否在迁移，如果在迁移就先把写命令queue起来
    int migrationCount = 0;

//    protected Set<Server> Servers = ConcurrentHashMap.newKeySet();
//    protected  Set<Table> Tables = ConcurrentHashMap.newKeySet();


    /**
     * TODO
     * 什么时候改它
     */

    protected Map<String, LinkedList<String>> tableLocations = new ConcurrentHashMap<>();// <表名, [主本服务器, 副本服务器,副本服务器]>
    protected Map<String, ArrayList<HashSet<String>>> serverTables = new ConcurrentHashMap<>(); // <服务器,主本表名和副本表名>
    // 负载均衡使用
    protected LoadBalanceManager loadBalanceManager;

    public TableManager() {
        loadBalanceManager = new LoadBalanceManager(this);
    }

    protected  Map<String,String> portMap = new ConcurrentHashMap<>();


    /**TODO
     * 增加新的table, 副本数量根据目前存活的服务器数量决定

     */
    public ArrayList<String> getNewTableServers(){

        int copycnt;
        synchronized (this){
            int size = serverCount();

            log.info("get new table servers: server count:"+size);
            if(size>=4){
                copycnt = 3;
            }else if(size  == 3 || size ==2){
                copycnt = 2;
            }else if(size == 1){
                copycnt =1;
            }else{
                copycnt = 0;
            }
            if (copycnt !=0) {
                return      getKMinTables(copycnt);
            }
        }
        if(copycnt == 0){
            return null;
        }

        // 先计算副本的数量
        return new ArrayList<>();

    }

    /**
     *
     * 找最少的k个table
     * 返回的第一个是主
     * @param k
     * @return
     */
    private ArrayList<String> getKMinTables(int k){
        /**TODO
         *
         */
        PriorityQueue<Server> ServerTableHeap = new PriorityQueue<>(
            new Comparator<Server>() {
                private Random random = new Random();
                double probability =0.5;
                /**
                 * TODO 怎么同步
                 *
                 */
                @Override
                public int compare(Server o1, Server o2) {
                    Integer cnt1 = o1.tables.get(0).size()+o1.tables.get(1).size();

                    Integer cnt2 = o2.tables.get(0).size()+o2.tables.get(1).size();
                    if(cnt1==cnt2){
                        if(o1.visitCnt == o2.visitCnt){
                            double randomValue = random.nextDouble();
                            if(randomValue< probability){
                                return -1;
                            }else{
                                return 1;
                            }
                        }else{
                            return o1.visitCnt - o2.visitCnt;
                        }
                    }else{
                        return cnt1-cnt2;
                    }
                }
            }
        );
        for(String s :serverTables.keySet()){
            Server se = new Server(s,loadBalanceManager.GetaccessSumforServer(s)
                    ,serverTables.get(s));
            ServerTableHeap.offer(se);
        }
        // 返回n个
       Iterator<Server>iterator = ServerTableHeap.iterator();
       int i = 0;
       ArrayList<String> result = new ArrayList<>();
       while(iterator.hasNext() && i <k){
           result.add(iterator.next().ipaddr);
           i++;
       }

        return result;
    }
    /**
     * 删除失效server，应保证删除之前tableLocations已被更新
     *
     * @param addr
     */
    public void removeServer(String addr) {
        synchronized (this) {
            log.info("remove server: "+serverTables.size()+" "+addr);
            String []arr = addr.split(":");
            String ip_port = arr[0]+":"+arr[1]+":"+arr[2];
            serverTables.remove(ip_port);
            loadBalanceManager.removeRecord(ip_port);
            portMap.remove(ip_port);
            printTableManager();
        }
    }

    public void addServer(String addr) {

        // 确保原子性
        synchronized (this) {
//            tableLocations.put(addr, new LinkedList<String>());
            // ip:client post:master port:region port
            String []arr = addr.split(":");
            String ip_port = arr[0]+":"+arr[1]+":"+arr[2];
            serverTables.put(ip_port, new ArrayList<HashSet<String>>(2));
            //put, string + region port的映射
            portMap.put(ip_port,arr[3]);
            log.info("new server added: "+serverTables.size()+" "+addr);
            serverTables.get(ip_port).add(new HashSet<String>());
            serverTables.get(ip_port).add(new HashSet<String>());
            printTableManager();

        }

    }

    /**
     * TODO 同步
     * 1.把对应的server改了
     *
     * @param tbName
     */
    public void addTable(String tbName,List<String> servers){
        synchronized (this){
            tableLocations.put(tbName,new LinkedList<>(servers));
            log.info(tbName+" is put "+tableLocations.size());
            int i = 0;
            for(String ip: servers){
                if(i == 0){
                    //是主本
                    serverTables.get(ip).get(0).add(tbName);
                    i++;
                }else{
                    serverTables.get(ip).get(1).add(tbName);
                }

            }
            printTableManager();
        }

    }

    /**
     * TODO 没用过
     * @param tbName
     */
    public void removeTable(String tbName){
        synchronized (this){
            LinkedList<String> servers = tableLocations.get(tbName);
            tableLocations.remove(tbName);

            int i = 0;
            for(String ip: servers){
                if(i == 0){
                    //是主本
                    serverTables.get(ip).get(0).remove(tbName);
                    i++;
                }else{
                    serverTables.get(ip).get(1).remove(tbName);
                }

            }
        }
    }
    public String TableServersToString(String tbName,boolean isGetReadOnly){
        LinkedList<String> ips = new LinkedList<>();
        synchronized (this){
            log.info("table servers to string"+ tbName+" "+tableLocations.get(tbName));

            ips = tableLocations.get(tbName);
            if(ips != null){
                log.info("ips's size "+ips+" "+ips.size());

            }else{
                log.info("ips null");
            }

        }
        String result = "";
        int index = 0;
        if(ips != null){
            for(String ip :ips){
                if(isGetReadOnly && index == 0){
                    index++;
                    continue;
                }
                result = ip + " ";
            }
        }

        return result;
    }
    public int serverCountForTable(String tbName){
        return tableLocations.get(tbName).size();
    }

    public int serverCount() {
        synchronized (this){
            return serverTables.size();

        }
    }

    public void setStatus(MasterStatus status) {
        this.status = status;
    }

    /**
     * 迁移表格，将table从outRegion迁移到inRegion
     *
     * @param outRegion
     * @param inRegion
     * @param table
     */
    public void migrate(String outRegion, String inRegion, String table) {
        synchronized (this) {
            // 确定table原来是主本还是副本
            if (serverTables.get(outRegion).get(0).contains(table)) {
                // 主本，更新迁入服务器为主本服务器
                tableLocations.get(table).remove(outRegion);
                tableLocations.get(table).add(0, inRegion);
                serverTables.get(outRegion).get(0).remove(table);
                serverTables.get(inRegion).get(0).add(table);
            } else {
                // 副本，更新迁入服务器为副本服务器
                tableLocations.get(table).remove(outRegion);
                tableLocations.get(table).add(inRegion);
                serverTables.get(outRegion).get(1).remove(table);
                serverTables.get(inRegion).get(1).add(table);
            }
            // migrate成功，
            setStatus(MasterStatus.RUNNING);
            log.info("负载均衡迁移成功: "+outRegion+"->"+inRegion+": "+table);
            log.info("新的tableManager更新如下:");
            // debug
            printTableManager();

        }

    }

    /**
     * 容错容灾时的拷贝副本情境，把副本从outRegion拷贝到inRegion
     *
     * @param outRegion
     * @param inRegion
     * @param table
     */
    public void recoveryMigrate(String outRegion, String inRegion, String table) {
        log.info("更新迁移表格=====================");
        // 只会出现在还剩两个region的情况，因此增加的都是副本
        synchronized (this) {
            log.info("before:-----------------");
            printTableManager();
            tableLocations.get(table).add(inRegion);
            serverTables.get(inRegion).get(1).add(table);
            log.info("容错容灾副本迁移成功："+outRegion+"->"+inRegion+" "+table);
            // 减少仍需迁移的表格数量
            this.migrationCount--;
            log.info("还需要迁移的表格数量是： "+this.migrationCount);
            if(this.migrationCount==0){
                // 迁移完毕，重新设置状态为running
                setStatus(MasterStatus.RUNNING);
                log.info("容错容灾副本迁移成功完成:");
            }
            log.info("after:-----------------");

            // debug
            printTableManager();

        }
    }

    /**
     * 找到不含指定表名的region
     *
     * @param table
     */
    public String findRegionWithoutTable(String table) {
        String ip = null;
        synchronized (this) {
            for (String server : serverTables.keySet()) {
                if (!tableLocations.get(table).contains(server)) {
                    ip = server;
                    break;
                }
            }
        }
        log.info("不含表"+table+"的region server是: "+ip);
        return ip;
    }

    public void setMigrationCount(int cnt) {
        synchronized (this){
            this.migrationCount = cnt;
        }

    }
    /**
     * used for debug
     */
    public void printTableManager(){
        synchronized (this) {
            System.out.println("tableLocations=================================");
            for (Map.Entry<String, LinkedList<String>> e : tableLocations.entrySet()) {
                System.out.println("table" + e.getKey() + ": ");
                for (String server : e.getValue()) {
                    System.out.println(server);
                }
            }
            System.out.println("serverTables==================================");
            for (Map.Entry<String, ArrayList<HashSet<String>>> e : serverTables.entrySet()) {
                System.out.println("server" + e.getKey() + ": ");
                System.out.println("maintable:");
                for (String maintable : e.getValue().get(0)) {
                    System.out.println(maintable);
                }
                System.out.println("copytable:");
                for (String maintable : e.getValue().get(1)) {
                    System.out.println(maintable);
                }
                System.out.println("---------------------------");
            }
        }

    }
    public boolean CheckTableExits(String tbName){
        boolean isExit = false;
        synchronized (this){
            if(tableLocations.containsKey(tbName)){
                isExit = true;
            }
        }
        return isExit;
    }

    /**
     * 将ip:region_port转成ip:client_port:master_port
     * @param regionPort
     * @return
     */
    String convertRegionPort2FullIP(String regionPort){
        String []arr = regionPort.split(":");
        String region_port = arr[1];
        Set<String> servers = serverTables.keySet();
        for(String s:servers){
            // 查找match的,s是ip:client_port:master_port
            if(s.contains(arr[0])){
                // 如果ip匹配，看port是否匹配
                if(portMap.get(s).equals(region_port)){

                    return s;
                }
            }
        }
        return null;

    }
    String getRegionPortIp(String full_ip){
        String ret = "";

        synchronized (this){
            String port  = portMap.get(full_ip);
            String []arr = full_ip.split(":");
            ret = arr[0]+":"+port;
        }
        return ret;
    }

}
