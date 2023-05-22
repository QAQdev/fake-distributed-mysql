package utils;

import org.json.*;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

public class RegionUtil {
    public RegionUtil() {
        ;
    }

    public static String resultSetToJson(ResultSet resultSet) throws Exception {
        JSONArray jsonArray = new JSONArray();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (resultSet.next()) {
            JSONObject jsonObject = new JSONObject();
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnName(i);
                Object value = resultSet.getObject(i);
                jsonObject.put(columnName, value);
            }
            jsonArray.put(jsonObject);
        }
        return jsonArray.toString(2);
    }

    // 将ResultSet所有记录的给定属性写在一行，以空格分隔
    public static String resultSetAttributeExtraction(ResultSet resultSet, String attribute) throws Exception {
        StringBuilder stringBuilder = new StringBuilder();
        while (resultSet.next()) {
            String tableName = resultSet.getString(attribute);
            stringBuilder.append(tableName).append(" ");
        }
        return stringBuilder.toString();
    }

    // 编码region-region协议字符串时，对协议字段包含的分隔符进行保护
    public static String protectOriginalSeparator(String original) {
        return original.replace("丨", "丨丨");
    }

    // 解码region-region协议字符串时，对协议字段包含的分隔符进行恢复
    public static String recoverOriginalSeparator(String original) {
        return original.replace("丨丨", "丨");
    }
}
