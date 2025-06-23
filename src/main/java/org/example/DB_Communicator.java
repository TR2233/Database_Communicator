package org.example;

import org.example.DB_Enums.Security;
import org.example.DB_Enums.Time_Frame;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class DB_Communicator {

    private static final ProcessBuilder googleAuthProxyProcessBuilder =
            new ProcessBuilder(StringResources.PROXY_EXECUTABLE_PATH, StringResources.PORT_PARAMETER, StringResources.PROJECT_NAME)
                    .inheritIO();
    private static Process googleAuthProxyProcess = null;


    public static synchronized List<List<String>> retrieveCandleData(Map<String, List<String>> timeFrameAndSecuritiesMap, LocalDateTime startLocalDate, LocalDateTime endLocalDate) throws IOException, InterruptedException {


        //Transform given string map into corresponding TimeFrame Securities map
        Map<Time_Frame, List<Security>> db_TimeFrameAndSecuritiesMap = timeFrameAndSecuritiesMap.entrySet()
                .stream()
                .collect(Collectors
                        .toMap((entry -> Time_Frame.valueOf(entry.getKey())), entry -> entry.getValue().stream().map(Security::valueOf).toList()));


        if (googleAuthProxyProcess == null) {
            googleAuthProxyProcess = googleAuthProxyProcessBuilder.start();
            googleAuthProxyProcess.waitFor(2000, TimeUnit.MILLISECONDS);
        }



        return null;
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
