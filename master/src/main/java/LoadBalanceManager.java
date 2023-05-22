import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @Description TODO
 **/
@Slf4j
public class LoadBalanceManager {
    //感觉不需要使用conccurentHashMap,因为为了保证原子性必须要加锁？
    // <服务器名称，<表格名，访问次数>>
    protected HashMap<String, HashMap<String, Integer>> record = new HashMap<>();
    // <服务器名称，访问次数总和>
    protected HashMap<String, Integer> accessSum = new HashMap<>();

    private static Integer THRESHOLD = 3;

    private TableManager tableManager;

    LoadBalanceManager(TableManager tableManager) {
        this.tableManager = tableManager;
    }
    public int GetaccessSumforServer(String server){
        if(!accessSum.containsKey(server)){
            return Integer.MAX_VALUE;
        }else{
            return accessSum.get(server);
        }
    }

    public void printLoadBalance(){
        System.out.println("loadbalance =======================================");
        synchronized (this){
            for(Map.Entry<String,HashMap<String, Integer>>e:record.entrySet()){
                System.out.println("server---");
                System.out.println(e.getKey()+" "+accessSum.get(e.getKey()));
                for(Map.Entry<String,Integer>en:e.getValue().entrySet()){
                    System.out.println("table: "+en.getKey()+" "+en.getValue());
                }
            }
        }

    }
    /**
     * 增加服务器记录
     *
     * @param addr    服务器地址
     * @param table1  第一个表格名称
     * @param access1 第一个表格访问次数
     * @param table2  第二个表格名称
     * @param access2 第二个表格访问次数
     */
    void updateRecord(String addr, String table1, Integer access1, String table2, Integer access2) {
        log.info("get load balance!"+addr+" "+table1+" "+access1+" "+table2+" "+access2);
        HashMap<String, Integer> hashmap = new HashMap<>();

        hashmap.put(table1, access1);
        hashmap.put(table2, access2);
        synchronized (this) {
            record.remove(addr);
            record.put(addr, hashmap);
            accessSum.remove(addr);
            accessSum.put(addr, access1 + access2);
        }
    }

    /**
     * 删除服务器记录
     *
     * @param addr
     */
    void removeRecord(String addr) {
        synchronized (this) {
            record.remove(addr);
            accessSum.remove(addr);
        }

    }

    /**
     * @return 可接收迁移表格的服务器地址
     */
    public String legalRegion(String table) {
        if(table==null) {
            return null;
        }
        //找到不含该表的region且该表访问次数之和小于threshold/3
        String address = tableManager.findRegionWithoutTable(table);
        if(address!=null&&accessSum.get(address) < THRESHOLD / 3 ){
            return address;
        }else{
            return null;
        }
    }

    /**
     * 策略：如果某节点的两个表访问次数加起来达到threshold次以上，其中一个表的访问次数达到threshold/3以上，
     * 且存在服务器两个访问数之和<threshold/3，则进行迁移
     *
     * @param addr
     * @return [接收迁移表格的服务器地址，需要迁移的表格名称]，不需要迁移则为null
     */
    public ArrayList<String> migrateToRegion(String addr) {
        // 这里的addr是ip:cleintport:masterport
        ArrayList<String> ret = new ArrayList<>();
        synchronized (this) {
            //如果访问次数之和大于threshold，查看是否有表格的访问次数>threshold/3
            if (accessSum.get(addr) > THRESHOLD) {
                Set<String> tables = record.get(addr).keySet();
                boolean need_migration = false;
                for (String t : tables) {
                    log.info("in loadbalacne: table: int "+t+" "+record.get(addr).get(t) );
                    if (t!=null && record.get(addr).get(t) > THRESHOLD / 3) {
                        need_migration = true;
                        break;
                    }
                }
                // 如果有表格的访问次数>threshold/3
                if (need_migration) {
                    // 遍历访问次数最多的表格，寻找可以接收迁移表格的服务器
                    for (String t : tables) {
                        // region是ip:clientport:masterport
                        String region = legalRegion(t);
                        if (region != null) {
                            String region_port = tableManager.portMap.get(region);
                            String [] arr = region.split(":");
                            // 找到，添加
                            ret.add(arr[0]+":"+region_port);
                            ret.add(t);
                            break;
                        }
                    }
                }
            }
        }
        return ret.size() == 0 ? null : ret;
    }
}
