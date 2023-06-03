import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.util.JdbcConstants;
import com.mysql.cj.jdbc.MysqlXADataSource;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import utils.CacheUtil;
import utils.RegionUtil;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.sql.XAConnection;
import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.fasterxml.jackson.core.json.JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS;


// 通过JDBC获取数据库元数据，包含表信息、索引信息
public class DatabaseManager {
    Connection connection;

    DatabaseMetaData databaseMetaData;
    XAConnection xaConnection;

    private Logger logger;

    private final String DB_NAME = "test03";

    private String dbUrl = "jdbc:mysql://localhost:3306/" + DB_NAME;

    private String user = "root";

    private String password = "00000000";

    public DatabaseManager() throws SQLException {
        logger = Logger.getLogger(DatabaseManager.class);
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            logger.log(Level.INFO, "JDBC import error: " + e);
        }

        connection = DriverManager.getConnection(dbUrl, user, password);
        databaseMetaData = connection.getMetaData();

        try {
            // 获取XAConnection对象
            MysqlXADataSource ds = new MysqlXADataSource();
            ds.setUrl(dbUrl);
            ds.setUser(user);
            ds.setPassword(password);
            xaConnection = ds.getXAConnection();
            logger.log(Level.INFO, "XA Connection created");

        } catch (Exception e) {
            logger.info("XA Connection creation failed. " + e);
        }

    }


    public String getMetaInfo() throws Exception {
        ResultSet resultSet = databaseMetaData.getTables(DB_NAME, null, null, null);
        return RegionUtil.resultSetAttributeExtraction(resultSet, "TABLE_NAME");
    }

    public String getTableName(String sql) {
        String dbType = JdbcConstants.MYSQL;
        // 格式化输出
        String format = SQLUtils.format(sql, dbType);

        List<SQLStatement> sqlStatements = SQLUtils.parseStatements(sql, dbType);

        // 独立语句的个数
        int size = sqlStatements.size();

        StringBuilder stringBuilder = new StringBuilder();

        for (SQLStatement stmt : sqlStatements) {
            MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
            stmt.accept(visitor);
            stringBuilder.append(visitor.getTables().keySet()).append(" ");
        }
        return stringBuilder.substring(1, stringBuilder.length() - 2);
    }

    public String showCreateTable(String table) throws Exception {

        Statement statement = this.connection.createStatement();
        String sql = "show create table " + table + ";";
        ResultSet resultSet = statement.executeQuery(sql);
        String ret = RegionUtil.resultSetAttributeExtraction(resultSet, "Create Table");
        statement.close();
        return ret;
    }

    public String showCopyTable(String table) throws Exception {
        Statement statement = this.connection.createStatement();
        String sql = "select * from " + table + ";";
        String jsonStr = RegionUtil.resultSetToJson(statement.executeQuery(sql));
        statement.close();
        return jsonStr;
    }

    public String showReconstructTable(String table) {
        try {
            String ret = RegionUtil.protectOriginalSeparator(table) + " 丨 " +
                    RegionUtil.protectOriginalSeparator(this.showCreateTable(table)) + " 丨 " +
                    RegionUtil.protectOriginalSeparator(this.showCopyTable(table));
            logger.log(Level.INFO, "showReconstructTable succeeded.");
            return ret;
        } catch (Exception e) {
            logger.log(Level.INFO, "showReconstructTable failed: " + e);
            return "";
        }
    }


    public int[] insertJsonRecords(String tableName, String jsonStr) {

        int rowsInserted = 0;
        int columnsCount = 0;


        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(), true);
        try {

            JsonNode root = mapper.readTree(jsonStr);
            Iterator<JsonNode> records = root.elements();
            while (records.hasNext()) {
                JsonNode record = records.next();
                Iterator<String> fieldNames = record.fieldNames();
                StringBuilder fieldsBuilder = new StringBuilder();
                StringBuilder valuesBuilder = new StringBuilder();
                while (fieldNames.hasNext()) {
                    String fieldName = fieldNames.next();
                    fieldsBuilder.append(fieldName).append(",");
                    valuesBuilder.append("?").append(",");
                }
                String fields = fieldsBuilder.substring(0, fieldsBuilder.length() - 1);
                String values = valuesBuilder.substring(0, valuesBuilder.length() - 1);
                columnsCount = values.split(",").length;

                String sql = "INSERT INTO " + tableName + " (" + fields + ") VALUES (" + values + ")";
                PreparedStatement statement = this.connection.prepareStatement(sql);
                int index = 1;
                Iterator<JsonNode> fieldValues = record.elements();
                while (fieldValues.hasNext()) {
                    JsonNode fieldValue = fieldValues.next();
                    if (fieldValue.isTextual()) {
                        statement.setString(index++, fieldValue.asText());
                    } else if (fieldValue.isInt()) {
                        statement.setInt(index++, fieldValue.asInt());
                    } else if (fieldValue.isDouble()) {
                        statement.setDouble(index++, fieldValue.asDouble());
                    } else if (fieldValue.isBoolean()) {
                        statement.setBoolean(index++, fieldValue.asBoolean());
                    }
                }
                rowsInserted += statement.executeUpdate();
            }
        } catch (SQLException | IOException e) {
            logger.log(Level.INFO, "Json insertion failed: " + e.getMessage());
        }
        return new int[]{rowsInserted, columnsCount};
    }


    public String reconstructTable(String command) throws Exception {
        // String[] commands is {table_name, create_table_sql, json_string}
        String[] commands = command.split(" 丨 ");

        // 对中文转义字符"丨（shu, gun）"进行还原
        commands[0] = RegionUtil.recoverOriginalSeparator(commands[0]);
        commands[1] = RegionUtil.recoverOriginalSeparator(commands[1]);
        commands[2] = RegionUtil.recoverOriginalSeparator(commands[2]);

        // 建表
        Statement statement = this.connection.createStatement();

        statement.executeUpdate(commands[1]);
        // 插入数据
        int[] successCount = insertJsonRecords(commands[0], commands[2]);

        // 结束句柄
        statement.close();

        CacheUtil.getInstance().listenNewTable(commands[0], this.getMetaInfo());

        logger.log(Level.INFO, "Reconstruction succeeded: " + successCount[0] + " rows, " + successCount[1] + " columns.");


        return commands[0];
    }


    // borrowed from cyy

    private Map<String, Integer> maxlen;

    private void printNode(JsonNode node) {
        getMaxLen(node);
        if (node.isObject()) {
            StringBuilder sb = new StringBuilder();
            node.fields().forEachRemaining(entry -> {
                String val = entry.getValue().asText();
                System.out.print(val);
                sb.append(val);
                for (int i = 0; i < maxlen.get(entry.getKey()) - val.length() + 3; i++) {
                    if (i == maxlen.get(entry.getKey()) - val.length() + 1) {
                        System.out.print('|');
                        sb.append('|');
                    } else {
                        System.out.print(' ');
                        sb.append(' ');
                    }
                }
            });
            System.out.println();
//            add_Log(sb.toString());
        } else if (node.isArray()) {
            JsonNode tmp = node.get(0);
            StringBuilder sb = new StringBuilder();
            tmp.fields().forEachRemaining(entry -> {
                String field = entry.getKey();
                System.out.print(field);
                sb.append(field);
                for (int i = 0; i < maxlen.get(field) - field.length() + 3; i++) {
                    if (i == maxlen.get(field) - field.length() + 1) {
                        System.out.print('|');
                        sb.append('|');
                    } else {
                        System.out.print(' ');
                        sb.append(' ');
                    }
                }
            });
//            add_Log(sb.toString());
            System.out.println();
            for (JsonNode elem : node) {
                printNode(elem);
            }
        }
    }

    private void getMaxLen(JsonNode node) {
        maxlen = new HashMap<>();
        if (node.isObject()) {
            node.fields().forEachRemaining(entry -> maxlen.put(entry.getKey(), Math.max(maxlen.getOrDefault(entry.getKey(), 0), entry.getValue().asText().length())));
        } else if (node.isArray()) {
            for (JsonNode elem : node) {
                getMaxLen(elem);
            }
        }
    }
}
