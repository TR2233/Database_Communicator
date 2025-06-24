package org.example;

import org.example.DB_Enums.CandleDataPoint;
import org.example.DB_Enums.Security;
import org.example.DB_Enums.Time_Frame;

import java.io.IOException;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DB_Communicator {

    private static final ProcessBuilder googleAuthProxyProcessBuilder =
            new ProcessBuilder(StringResources.PROXY_EXECUTABLE_PATH, StringResources.PORT_PARAMETER, StringResources.PROJECT_NAME)
                    .inheritIO();
    private static Process googleAuthProxyProcess = null;


    public static synchronized Map<String, Map<String, List<List<String>>>> retrieveCandleData(Map<String, List<String>> timeFrameAndSecuritiesMap, LocalDateTime startLocalDate, LocalDateTime endLocalDate) throws IOException, InterruptedException, SQLException {

        Map<String, Map<String, List<List<String>>>> resultSetMap = new HashMap<>();

        // Initialize the google auth proxy process and open the database connection//
        googleAuthProxyProcess = googleAuthProxyProcessBuilder.start();
        googleAuthProxyProcess.waitFor(2000, TimeUnit.MILLISECONDS);
        Connection db_connection = DriverManager.getConnection(StringResources.DB_URL, StringResources.DB_USER, StringResources.DB_PASSWORD);
        Statement statement = db_connection.createStatement();

        //begin database query and map population//
        timeFrameAndSecuritiesMap.forEach((timeFrame, securities) -> {
            Time_Frame db_TimeFrame = Time_Frame.valueOf(timeFrame);
            resultSetMap.put(db_TimeFrame.toString(), new HashMap<>());
            securities.forEach(security -> {
                Security db_Security = Security.valueOf(security);
                resultSetMap.get(timeFrame).put(db_Security.toString(), new ArrayList<>());//table/rows for the security name
                String query = String.format("select * from \"%s\" where \"TIMESTAMP\" >= '%s' and \"TIMESTAMP\" < '%s' order by \"TIMESTAMP\""
                        , db_TimeFrame.getDatabaseAbbreviation(db_Security.toString()), startLocalDate, endLocalDate);
                try (ResultSet resultSet = statement.executeQuery(query)) {
                    while (resultSet.next()) {
                        resultSetMap.get(db_TimeFrame.toString())
                                .get(db_Security.toString())
                                .add(getDb_CandleResult(resultSet));
                    }
                } catch (SQLException e) {
                    googleAuthProxyProcess.destroy();
                    throw new RuntimeException(e);
                }
            });
        });
        statement.close();
        db_connection.close();
        googleAuthProxyProcess.destroy();
        return resultSetMap;
    }

    private static List<String> getDb_CandleResult(ResultSet resultSet) throws SQLException {
        List<String> db_RowResult = new ArrayList<>();
        db_RowResult.add(resultSet.getString(CandleDataPoint.TICKER.toString()));
        db_RowResult.add(resultSet.getTimestamp(CandleDataPoint.TIMESTAMP.toString()).toLocalDateTime().toString());
        db_RowResult.add(String.valueOf(resultSet.getDouble(CandleDataPoint.OPEN.toString())));
        db_RowResult.add(String.valueOf(resultSet.getDouble(CandleDataPoint.HIGH.toString())));
        db_RowResult.add(String.valueOf(resultSet.getDouble(CandleDataPoint.LOW.toString())));
        db_RowResult.add(String.valueOf(resultSet.getDouble(CandleDataPoint.CLOSE.toString())));
        db_RowResult.add(String.valueOf(resultSet.getDouble(CandleDataPoint.PREVIOUS_CLOSE.toString())));
        db_RowResult.add(String.valueOf(resultSet.getDouble(CandleDataPoint.VOLUME.toString())));
        return db_RowResult;
    }
}
