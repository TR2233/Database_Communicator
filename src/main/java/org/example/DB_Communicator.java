package org.example;

import org.example.DB_Enums.Security;
import org.example.DB_Enums.Time_Frame;

import java.io.IOException;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class DB_Communicator {

    private static final ProcessBuilder googleAuthProxyProcessBuilder =
            new ProcessBuilder(StringResources.PROXY_EXECUTABLE_PATH, StringResources.PORT_PARAMETER, StringResources.PROJECT_NAME)
                    .inheritIO();
    private static Process googleAuthProxyProcess = null;


    public static synchronized Map<String, List<ResultSet>> retrieveCandleData(Map<String, List<String>> timeFrameAndSecuritiesMap, LocalDateTime startLocalDate, LocalDateTime endLocalDate) throws IOException, InterruptedException, SQLException {


        final Map<String, List<ResultSet>> resultSetMap = new HashMap<>();
        //Transform given string map into corresponding TimeFrame Securities map
        //this will throw and error if the given values of string map do not map the values of enums after turning them to strings
        Map<Time_Frame, List<Security>> db_TimeFrameAndSecuritiesMap = timeFrameAndSecuritiesMap.entrySet()
                .stream()
                .collect(Collectors
                        .toMap((entry -> Time_Frame.valueOf(entry.getKey())), entry -> entry.getValue().stream().map(Security::valueOf).toList()));


        if (googleAuthProxyProcess == null) {
            googleAuthProxyProcess = googleAuthProxyProcessBuilder.start();
            googleAuthProxyProcess.waitFor(2000, TimeUnit.MILLISECONDS);
        }
        Connection db_connection = DriverManager.getConnection(StringResources.DB_URL, StringResources.DB_USER, StringResources.DB_PASSWORD);

        Statement statement = db_connection.createStatement();
        db_TimeFrameAndSecuritiesMap.forEach((timeFrame, securities) -> {

            securities.forEach(security -> {

                String query = String.format("select * from \"%s\" where \"TIMESTAMP\" >= '%s' and \"TIMESTAMP\" < '%s' order by \"TIMESTAMP\""
                        , timeFrame.getDatabaseAbbreviation(security.toString()), startLocalDate, endLocalDate);
                try {
                    if (!resultSetMap.containsKey(timeFrame.toString())) {
                        resultSetMap.put(timeFrame.toString(), new ArrayList<>());
                    }
                    resultSetMap.get(timeFrame.toString()).add(statement.executeQuery(query));
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
        });

        statement.close();
        db_connection.close();
        googleAuthProxyProcess.destroy();
        return resultSetMap;
    }

//    private static void initiateGoogleAuthProxy() {
//        String dbUrl = "jdbc:postgresql://localhost:6000/testDataBase";
//        String dbUser = "postgres";
//        String dbPassword = "Federer!66";
//        Process googleAuthProxyProcess = null;
//        Connection connection;
//        try {
//            googleAuthProxyProcess = processBuilder.start();
//            googleAuthProxyProcess.waitFor(2000, TimeUnit.MILLISECONDS);
//            connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
//            System.out.println("Successfully connected to the database!");
//
////            updateTable(connection, getLocalData());
//            connection.close();
//            googleAuthProxyProcess.destroy();
////            createTable(connection);
//        } catch (SQLException | IOException e) {
//            if (googleAuthProxyProcess != null) {
//                googleAuthProxyProcess.destroy();
//            }
//            throw new RuntimeException(e);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//
//    }
}
