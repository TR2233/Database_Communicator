package org.example;

import org.example.DB_Enums.CandleDataPoint;
import org.example.DB_Enums.Security;
import org.example.DB_Enums.Time_Frame;

import java.io.IOException;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;

public class DB_Communicator {

    private static DB_Communicator instance;
    private final ProcessBuilder googleAuthProxyProcessBuilder =
            new ProcessBuilder(StringResources.PROXY_EXECUTABLE_PATH, StringResources.PORT_PARAMETER, StringResources.PROJECT_NAME)
                    .inheritIO();
    private Process googleAuthProxyProcess = null;
    private Connection db_Connection = null;

    private DB_Communicator() {
    }

    public static synchronized DB_Communicator getInstance() {
        if (instance == null) {
            instance = new DB_Communicator();
        }
        return instance;
    }

    public void connectToDatabase(String security) {
        try {
            googleAuthProxyProcess = googleAuthProxyProcessBuilder.start();
            googleAuthProxyProcess.waitFor(2000, TimeUnit.MILLISECONDS);
            db_Connection = DriverManager.getConnection(createDB_URL(security), StringResources.DB_USER, StringResources.DB_PASSWORD);
        } catch (IOException | InterruptedException | SQLException e) {
            googleAuthProxyProcess.destroy();
            throw new RuntimeException(e);
        }
        System.out.printf("SUCCESSFULLY STARTED GOOGLE AUTH PROXY PROCESS AND CONNECTED TO %s HISTORICAL DATABASE%n", security);
    }

    private String createDB_URL(String security) {


        return new StringJoiner("/").add(StringResources.DB_URL).add(Security.valueOf(security).toString()).toString();
    }

    public void disconnectFromDatabase() {
        try {
            db_Connection.close();
        } catch (SQLException e) {
            System.out.println("COULDNT CLOSE DATABASE CONNECTION, CONTINUING TO SHUTDOWN GOOGLE AUTH PROXY PROCESS");
        }
        googleAuthProxyProcess.destroy();
        System.out.println("SUCCESSFULLY SHUTDOWN GOOGLE AUTH PROXY PROCESS");
    }


    public synchronized List<List<String>> retrieveCandleData(String timeframe, LocalDateTime startLocalDate, LocalDateTime endLocalDate) {
        List<List<String>> candleValues = new ArrayList<>();

        if (!googleAuthProxyProcess.isAlive()) {
            System.out.println("GOOGLE AUTH PROXY PROCESS NOT RUNNING!");
            System.out.println("RETURNING EMPTY LIST OF CANDLE DATA!");
        } else {

            try {
                String query = String.format("select * from \"%s\" where \"DATE_TIME\" >= '%s' and \"DATE_TIME\" <= '%s' order by \"DATE_TIME\""
                        , Time_Frame.valueOf(timeframe).getDatabaseAbbreviation(), startLocalDate, endLocalDate);
                ResultSet resultSet = db_Connection.createStatement().executeQuery(query);
                while (resultSet.next()) {
                    candleValues.add(getDb_CandleResult(resultSet));
                }
            } catch (SQLException | IllegalArgumentException e) {
                if (googleAuthProxyProcess.isAlive()) {
                    googleAuthProxyProcess.destroy();
                }
                throw new RuntimeException(e);
            }
        }
        System.out.println("CANDLE RETRIEVAL SUCCESSFUL. RETURNING THE DATA NOW");
        return candleValues;
    }

    private List<String> getDb_CandleResult(ResultSet resultSet) throws SQLException {
        List<String> db_RowResult = new ArrayList<>();
        db_RowResult.add(resultSet.getString(CandleDataPoint.SYMBOL.toString()));
        db_RowResult.add(resultSet.getTimestamp(CandleDataPoint.DATE_TIME.toString()).toLocalDateTime().toString());
        db_RowResult.add(String.valueOf(resultSet.getDouble(CandleDataPoint.OPEN.toString())));
        db_RowResult.add(String.valueOf(resultSet.getDouble(CandleDataPoint.HIGH.toString())));
        db_RowResult.add(String.valueOf(resultSet.getDouble(CandleDataPoint.LOW.toString())));
        db_RowResult.add(String.valueOf(resultSet.getDouble(CandleDataPoint.CLOSE.toString())));
        db_RowResult.add(String.valueOf(resultSet.getDouble(CandleDataPoint.PREVIOUS_CLOSE.toString())));
        db_RowResult.add(String.valueOf(resultSet.getDouble(CandleDataPoint.VOLUME.toString())));
        return db_RowResult;
    }

    //must be in the order of ticker, timestamp, open, high, low, close, previous_close, volume!
    // I understand that this is poor design and perhaps later I can remedy it somehow, I hope
    // Outer map key value is Timeframe and inner map key is ticker name
    private synchronized void updateCandleDatabaseTable(String timeframe, List<List<String>> suppliedCandleValues) {
        if (!googleAuthProxyProcess.isAlive()) {
            System.out.println("GOOGLE AUTH PROXY PROCESS NOT RUNNING!");
            System.out.println("CLOUD DATA NOT UPDATED!");
        } else {

            LocalDateTime earliestDate = LocalDateTime.parse(suppliedCandleValues.getFirst().get(1));
            LocalDateTime latestDate = LocalDateTime.parse(suppliedCandleValues.getLast().get(1));
            System.out.println(latestDate);
            List<LocalDateTime> currentCandleDatabaseTimeStamps = retrieveCandleData(timeframe, earliestDate, latestDate).stream().map(tableRow -> LocalDateTime.parse(tableRow.get(1))).toList();
            List<List<String>> filteredSuppliedCandleRows = suppliedCandleValues.stream()
                    .filter(candleValues -> Collections.binarySearch(currentCandleDatabaseTimeStamps, LocalDateTime.parse(candleValues.get(1))) < 0).toList();

            try {
                Statement statement = db_Connection.createStatement();
                for (List<String> row : filteredSuppliedCandleRows) { //seems table and column names must be surrounded by quotes
                    String format = String.format("insert into \"%s\"(\"TICKER\", \"DATE_TIME\", \"OPEN\", \"HIGH\", \"LOW\", \"CLOSE\", \"PREVIOUS_CLOSE\", \"VOLUME\") values (%s)"
                            , Time_Frame.valueOf(timeframe)
                            , String.format("'%s','%s',%s", row.get(0), row.get(1), row.subList(2, row.size()).toString().replaceAll("[\\[\\]]", ""))); // assumed column names with strings or timestamps must be surrounded by ' character
                    System.out.println(statement.executeUpdate(format));
                }
            } catch (SQLException | IllegalArgumentException e) {
                if (googleAuthProxyProcess.isAlive()) {
                    googleAuthProxyProcess.destroy();
                }
                throw new RuntimeException(e);
            }
            System.out.println("CANDLE INSERTION SUCCESSFUL");
        }
    }
}
