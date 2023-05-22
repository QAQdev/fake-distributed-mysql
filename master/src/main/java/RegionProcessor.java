import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;


/**
 * @Description TODO
 **/
@Slf4j
public class RegionProcessor {
    //要用到handler
    private Handler handler;

    public RegionProcessor(Handler handler) throws IOException {
        this.handler = handler;

    }

    /**
     * 处理来自region的消息
     *
     * @param cmd
     * @return
     */
    public String process(String cmd) {
        String result = "<master-region>";

        if (cmd.startsWith("[1]")) {
            log.info("region processor: recovery migration");
            // 容错容灾，副本复制信息
            result = processRecoveryMigration(cmd.substring(3).trim());
        } else if (cmd.startsWith("[2]")) {
            log.info("region processor: server access ");
            // region表格访问次数信息
            log.info("raw raw cmd "+ cmd);
            result = processLoadBalance(cmd.substring(3).trim());

        } else if (cmd.startsWith("[3]")) {
            log.info("region processor: load balance migration");

            // 负载均衡分区迁移状态信息
            result = processLBMigration(cmd.substring(3).trim());

        }
        return result;
    }

    public String processLoadBalance(String cmd) {
        String result = "";
        log.info("raw cmd "+ cmd);
        String[] arglist = cmd.split("\\s+");
//        cmd = cmd.trim();
//        String[] arglist = cmd.split(" ");

        log.info("loadbalance process: "+cmd+" "+arglist.length);

        String table1 = null;
        Integer access1 = 0;
        String table2 = null;
        Integer access2 = 0;

        switch(arglist.length){
            case 2:
                table1 = arglist[0];
                access1=Integer.valueOf(arglist[1]);
                break;
            case 4:
                table1 = arglist[0];
                access1=Integer.valueOf(arglist[1]);
                table2 = arglist[2];
                access2=Integer.valueOf(arglist[3]);

                break;
            default:
                log.info("wrong cmd length! ");

        }
        //更新统计信息
        handler.tableManager.loadBalanceManager.updateRecord(handler.getRegionFullIP(), table1, access1, table2, access2);
        String fullip = handler.getRegionFullIP(); //ip:clientport:masterport
        //查看是否需要迁移
        ArrayList<String> migration = handler.tableManager.loadBalanceManager.migrateToRegion(
                fullip
        );
        // 如果需要迁移，则发送迁移分区信息
        if (migration != null) {
            handler.tableManager.setStatus(MasterStatus.MIGRATING);
            log.info("进行热点迁移: "+fullip+" --> "+migration.get(0)+":"+migration.get(1));
            // migration 是迁入的服务器ip(region_port), table_name
            result = "<master-region>[3]" + migration.get(0) + " " + migration.get(1);

        }else{
            log.info("不需要负载均衡迁移表格！");
        }
        // used for debug
        handler.tableManager.loadBalanceManager.printLoadBalance();
        return result;
    }

    public String processLBMigration(String cmd) {
        // 分区迁移成功，更新元信息，失败就不用管了
        if (cmd.startsWith("true")) {
            String[] arglist = cmd.split("\\s+");

            //arglist[1]是ip:region_port
            String  inRegion = handler.tableManager.convertRegionPort2FullIP(arglist[1]);
            log.info("processLBMigration: get LBMigration status: "+inRegion);

            handler.tableManager.migrate(handler.getRegionFullIP(), inRegion, arglist[2]);
            //设置为running

        }else{
            log.info("migration fail!");
        }
        // 不需要返回结果
        return "";
    }

    public String processRecoveryMigration(String cmd) {
        String result = "";
        String[] arglist = cmd.split("\\s+");

        assert arglist.length == 3;
        // 分区迁移成功，更新元信息，失败要重新发送信息让它迁移，向client发送更新后的信息
        if (cmd.startsWith("true")) {
            // 需要将ip:region_port 转化为ip:client_port:master_port形式
            String  inRegion = handler.tableManager.convertRegionPort2FullIP(arglist[1]);
            log.info("成功迁移了！"+inRegion);
            //log.info("processRecoveryMigration: "+inRegion);

            handler.tableManager.recoveryMigrate(handler.getRegionFullIP(),inRegion, arglist[2]);

            //            handler.tableManager.setStatus(MasterStatus.RUNNING);

        } else {
            log.info("失败了！");
            // 继续发送信息使其迁移
            result = "<master-region>[1]" + arglist[1] + " " + arglist[2];
        }
        return result;

    }


}
