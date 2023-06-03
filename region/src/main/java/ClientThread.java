import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.sql.*;

import utils.CommandHeader;
import utils.RegionUtil;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import utils.StatisticsUtil;

public class ClientThread implements Runnable {

    private Socket socket;
    private boolean isRunning;

    public BufferedReader input;
    public PrintWriter output;
    private Logger logger;

    private DatabaseManager databaseManager;

    public ClientThread(Socket socket, DatabaseManager databaseManager) throws IOException {
        this.socket = socket;
        this.isRunning = true;
        this.input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.output = new PrintWriter(socket.getOutputStream(), true);
        this.logger = Logger.getLogger(ClientThread.class);
        this.databaseManager = databaseManager;
        logger.log(Level.INFO, "Region 建立了新的客户端子线程：" + socket.getPort());
    }

    @Override
    public void run() {
        logger.log(Level.INFO, "Region 监听客户端消息中..." + socket.getInetAddress() + ":" + socket.getPort());

        try {
            while (isRunning) {
                String line = input.readLine();
                if (line != null) {
                    logger.log(Level.INFO, "接收到客户端消息：" + line);
                    execute(line.substring(CommandHeader.CLIENT_TO_REGION_1.value.length()));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                input.close();
                output.close();
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void execute(String sql) throws SQLException {
        logger.log(Level.INFO, "执行 sql 语句：" + sql);
        Connection connection = databaseManager.connection;
        Statement statement = connection.createStatement();

        try {
            ResultSet resultSet = statement.executeQuery(sql);
            int columnCount = resultSet.getMetaData().getColumnCount();
            String tableName = resultSet.getMetaData().getTableName(columnCount);
            logger.log(Level.INFO, tableName);

            // 更新访问频次
            StatisticsUtil.getInstance().update(tableName);

            StringBuffer ret = new StringBuffer(CommandHeader.REGION_TO_CLIENT_1.value + " ");

            ret.append(RegionUtil.resultSetToJson(resultSet));
            output.println(ret + "\nEND_OF_TRANSMISSION");
            logger.log(Level.INFO, ret + "\nEND_OF_TRANSMISSION");
            logger.log(Level.INFO, "执行 sql 语句成功");

        } catch (SQLException se) {
            // 获取并显示错误信息
            String errorMessage = se.getMessage();
            logger.error(CommandHeader.REGION_TO_CLIENT_1.value + " " + errorMessage);
            output.println(CommandHeader.REGION_TO_CLIENT_1.value + " " + errorMessage);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (statement != null) {
                    statement.close();
                }
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }
    }
}
