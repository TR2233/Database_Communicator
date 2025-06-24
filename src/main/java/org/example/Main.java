package org.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Main {

    public static void main(String[] args) {
        System.out.println("Start Program");
        Map<String, List<String>> testMap = new HashMap<>();
        List<String> testList = new ArrayList<>();
        testList.add("MES");
        testList.add("MES");
        testMap.put("FIVE_MINUTE", testList);
        try {
            Map<String, Map<String, List<List<String>>>> stringListMap = DB_Communicator.retrieveCandleData(testMap, LocalDateTime.of(2025, 1, 1, 0, 0), LocalDateTime.now());
        } catch (IOException | InterruptedException | SQLException e) {
            throw new RuntimeException(e);
        }
        System.out.println();
//        ProcessBuilder processBuilder = new ProcessBuilder("C:\\Google_Cloud\\cloud_sql_proxy.exe", "-p6000", "avid-catalyst-461411-d2:europe-west3:test-database")
//                .inheritIO();
//        runDatabaseCode(processBuilder);
    }

    private static void runDatabaseCode(ProcessBuilder processBuilder) {
        getLocalData();
        String dbUrl = "jdbc:postgresql://localhost:6000/testDataBase";
        String dbUser = "postgres";
        String dbPassword = "Federer!66";
        Process googleAuthProxyProcess = null;
        Connection connection = null;
        try {
            googleAuthProxyProcess = processBuilder.start();
            googleAuthProxyProcess.waitFor(2000, TimeUnit.MILLISECONDS);
            connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
            System.out.println("Successfully connected to the database!");

            updateTable(connection, getLocalData());
            connection.close();
            googleAuthProxyProcess.destroy();
//            createTable(connection);
        } catch (SQLException | IOException e) {
            if (googleAuthProxyProcess != null) {
                googleAuthProxyProcess.destroy();
            }
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<LocalDateTime> retrieveDatabase(Connection connection, List<LocalDateTime> localData) throws SQLException {

        String earliestDate = localData.get(0).toString();
        String latestDate = localData.getLast().toString();
        List<LocalDateTime> SQLTimeStamps = new ArrayList<>();
        String query = String.format("select * from \"MES_5MIN\" where \"TIMESTAMP\" >= '%s' and \"TIMESTAMP\" < '%s' order by \"TIMESTAMP\"", earliestDate, latestDate);
        System.out.println(earliestDate);
        System.out.println(latestDate);
        System.out.println(query);

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);

        while (resultSet.next()) {
            LocalDateTime timeStamp = resultSet.getTimestamp("TIMESTAMP").toLocalDateTime();
            SQLTimeStamps.add(LocalDateTime.of(timeStamp.toLocalDate(), timeStamp.toLocalTime()));
            System.out.println(SQLTimeStamps.getLast().toString());
        }
        System.out.format("Number of Timestamps: %d%n", SQLTimeStamps.size());
        statement.close();
        return SQLTimeStamps;
    }

    private static void createTable(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        String createTableStatement = "CREATE TABLE TASKS (id SERIAL PRIMARY KEY, name VARCHAR(255))";
        statement.execute(createTableStatement);
        statement.close();
        System.out.println("Table created!!!");
    }

    private static void updateTable(Connection connection, List<LocalDateTime> localData) throws SQLException {

        List<LocalDateTime> databaseEntries = retrieveDatabase(connection, localData);
        List<LocalDateTime> newDataToInsert = new ArrayList<>();

        outerLoop:
        for (LocalDateTime localDatum : localData) {
            for (LocalDateTime databaseEntry : databaseEntries) {
                if (localDatum.isEqual(databaseEntry)) {
                    continue outerLoop;
                }
            }
            newDataToInsert.add(localDatum);
        }

        Statement statement = connection.createStatement();
//        statement.execute()

    }

    private static List<LocalDateTime> getLocalData() {
        List<LocalDateTime> data = new ArrayList<>();

        try (BufferedReader bufferedReader =
                     new BufferedReader(new FileReader("C:\\Users\\treim\\Documents\\Historical_Stock_Data\\Tickers\\MES\\5M\\Formatted_CSV_Files\\final_Historical_Ticker_Data\\MES_2024_12_10_2025_06_13.csv"))) {

            bufferedReader.lines().forEach(line -> {
                if (line.contains(":")) {
                    data.add(LocalDateTime.parse(line.split(",")[1].replace("\"", "")));
                }
            });

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return data;
    }

    public static void powerShellScriptTest() {
        System.out.println("INITIATE POWERSHELL SCRIPT");
        try {
            String googleCloudAuthProxyDirectory = "C:\\Google_Cloud\\";
            String powershellDirectory = "C:/Windows/System32/WindowsPowerShell/v1.0";

            String googleCloudAuthProxyExe = "cloud_sql_proxy.exe";
            String powershellExe = "powershell.exe";

            String scriptHelloWorld = "C:\\Users\\treim\\Documents\\Historical_Stock_Data\\Scripts\\Hello_World.ps1";
            String scriptMakeDirectory = "C:\\Users\\treim\\Documents\\Historical_Stock_Data\\Scripts\\mkdirScript.ps1";


            Process process = new ProcessBuilder("C:\\Google_Cloud\\cloud_sql_proxy.exe", "-p6000", "avid-catalyst-461411-d2:europe-west3:test-database")
                    .inheritIO()
                    .start();
            process.waitFor(2000, TimeUnit.MILLISECONDS);
            ProcessHandle handle = process.toHandle();
            System.out.println(handle.info().toString());
            process.destroy();
        } catch (IOException e) {
//            Logger.getLogger(EndOfDayActivities.class.getName()).log(Level.SEVERE,null,e);
            System.out.println("Error thrown");
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}