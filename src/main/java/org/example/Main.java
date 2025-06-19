package org.example;

import java.io.*;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    static final String JDBC_URL = "jdbc:postgresql://localhost:5432/Historical_Data?currentSchema=public&user=postgres&password=Federer!66";
    //    public static final String POWERSHELL_PROCESS_INPUT = "\"C:\\WINDOWS\\system32\\WindowsPowerShell\\v1.0\\powershell.exe\" \"C:\\Users\\treim\\Documents\\Historical_Stock_Data\\Scripts\\TheAllFoldersSimplifiedHistoricalExtract.ps1\"";
    public static final String POWERSHELL_PROCESS_INPUT = "\"C:\\WINDOWS\\system32\\WindowsPowerShell\\v1.0\\powershell.exe\" \"C:\\Users\\treim\\Documents\\Historical_Stock_Data\\Scripts\\Hello_world.ps1\"";
    public static final String POWERSHELL_PROCESS_DIRECTORY = "\"C:\\WINDOWS\\system32\\WindowsPowerShell\\v1.0\\powershell.exe\" \"C:\\Users\\treim\\Documents\\Historical_Stock_Data\\Scripts\\Hello_world.ps1\"";


    public static void main(String[] args) {
        System.out.println("Start Program");

//        runDatabaseCode();
        powerShellScriptTest();
    }

    private static void runDatabaseCode() {
        getLocalData();
        String dbUrl = "jdbc:postgresql://localhost:6000/testDataBase";
        String dbUser = "postgres";
        String dbPassword = "Federer!66";
//        FileWriter fileWriter = new FileWriter("test.csv");
//        BufferedWriter bufferedWriter = new BufferedWriter();
        try (Connection connection = DriverManager.getConnection(dbUrl, dbUser, dbPassword)) {
//            ResultSet resultSet = retrieveDatabase(connection, getLocalData());
            updateTable(connection, retrieveDatabase(connection, getLocalData()));
//            createTable(connection);
            connection.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static ResultSet retrieveDatabase(Connection connection, List<LocalDateTime> localData) throws SQLException {

        String earliestDate = localData.get(0).toString();
        String latestDate = localData.getLast().toString();
        List<LocalDateTime> SQLTimeStamps = new ArrayList<>();
        String query = String.format("select * from \"MES_5MIN\" where \"TIMESTAMP\" >= '%s' and \"TIMESTAMP\" < '%s' order by \"TIMESTAMP\" DESC", earliestDate, latestDate);
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
        System.out.println("Successfully connected to the database!");
        statement.close();
        return resultSet;
    }

    private static void createTable(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        String createTableStatement = "CREATE TABLE TASKS (id SERIAL PRIMARY KEY, name VARCHAR(255))";
        statement.execute(createTableStatement);
        statement.close();
        System.out.println("Table created!!!");
    }

    private static void updateTable(Connection connection, ResultSet newEntries) throws SQLException {
        Statement statement = connection.createStatement();
//        String unionizeTables =
    }

    private static List<LocalDateTime> getLocalData() {
        List<LocalDateTime> data = new ArrayList<>();

        try (BufferedReader bufferedReader =
                     new BufferedReader(new FileReader("C:\\Users\\treim\\Documents\\Historical_Stock_Data\\Tickers\\MES\\5M\\Formatted_CSV_Files\\final_Historical_Ticker_Data\\MES_2024_12_10_2025_06_13.csv"));) {

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
           Process process =new ProcessBuilder()
                   .directory(new File("C:\\WINDOWS\\system32\\WindowsPowerShell\\v1.0"))
                   .command("powershell.exe", "C:\\Users\\treim\\Documents\\Historical_Stock_Data\\Scripts\\HelloWorld.ps1\"")
                   .start();
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            bufferedReader.lines().forEach(System.out::println);
        } catch (IOException e) {
//            Logger.getLogger(EndOfDayActivities.class.getName()).log(Level.SEVERE,null,e);
            System.out.println("Error thrown");
            throw new RuntimeException(e);
        }
    }
}