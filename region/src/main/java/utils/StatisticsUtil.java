package utils;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;

public class StatisticsUtil {

    private static StatisticsUtil statisticsHandle;

    // 表名：访问次数
    private static Map<String, Integer> statisticsMap = new LinkedHashMap<>();

    private StatisticsUtil() {

    }

    public static synchronized StatisticsUtil getInstance() {
        return Objects.requireNonNullElseGet(statisticsHandle, StatisticsUtil::new);
    }

    public void remove(String tableName) {
        statisticsMap.remove(tableName);
    }

    public void update(String tableName) {
        StatisticsUtil handle = getInstance();
        if (statisticsMap.containsKey(tableName)) {
            statisticsMap.put(tableName, statisticsMap.get(tableName) + 1);
        } else {
            statisticsMap.put(tableName, 1);
        }
    }

    public String getTopK(int k) {

        PriorityQueue<String> q = new PriorityQueue<>(k);
        for (Map.Entry<String, Integer> entry : statisticsMap.entrySet()) {
            if (q.size() < k) {
                q.offer(entry.getKey());
            } else {
                if (entry.getValue() > statisticsMap.get(q.peek())) {
                    q.poll();
                    q.offer(entry.getKey());
                }
            }
        }

        StringBuffer ret = new StringBuffer();
        while (!q.isEmpty()) {
            String tableName = q.poll();
            ret.append(tableName).append(" ").append(statisticsMap.get(tableName)).append(" ");
        }
        return ret.toString();
    }

    public void clear() {
        statisticsMap.clear();
    }
}
