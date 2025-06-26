package org.example;

import org.example.DB_Enums.CandleDataPoint;
import org.example.DB_Enums.Security;
import org.example.DB_Enums.Time_Frame;

import java.io.IOException;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class DB_Communicator {

    private static DB_Communicator instance;
    private final ProcessBuilder googleAuthProxyProcessBuilder =
            new ProcessBuilder(StringResources.PROXY_EXECUTABLE_PATH, StringResources.PORT_PARAMETER, StringResources.PROJECT_NAME)
                    .inheritIO();
    private Process googleAuthProxyProcess = null;
    private Connection db_Connection = null;
    private Statement db_Statement = null;

    private DB_Communicator() {
    }

    public static synchronized DB_Communicator getInstance() {
        if (instance == null) {
            instance = new DB_Communicator();
        }
        return instance;
    }


    public void connectToDatabase() {

        try {
            googleAuthProxyProcess = googleAuthProxyProcessBuilder.start();
            googleAuthProxyProcess.waitFor(2000, TimeUnit.MILLISECONDS);
            db_Connection = DriverManager.getConnection(StringResources.DB_URL, StringResources.DB_USER, StringResources.DB_PASSWORD);
        } catch (IOException | InterruptedException | SQLException e) {
            googleAuthProxyProcess.destroy();
            throw new RuntimeException(e);
        }
        System.out.println("SUCCESSFULLY STARTED GOOGLE AUTH PROXY PROCESS AND CONNECTED TO DATABASE");
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


    public synchronized List<List<String>> retrieveCandleData(String timeframe, String security, LocalDateTime startLocalDate, LocalDateTime endLocalDate) {
        List<List<String>> candleValues = new ArrayList<>();

        if (!googleAuthProxyProcess.isAlive()) {
            System.out.println("GOOGLE AUTH PROXY PROCESS NOT RUNNING!");
            System.out.println("RETURNING EMPTY LIST OF CANDLE DATA!");
        } else {

            String query = String.format("select * from \"%s\" where \"TIMESTAMP\" >= '%s' and \"TIMESTAMP\" < '%s' order by \"TIMESTAMP\""
                    , Time_Frame.valueOf(timeframe).getDatabaseAbbreviation(Security.valueOf(security).toString()), startLocalDate, endLocalDate);
            try {
                db_Statement = db_Connection.createStatement();
                ResultSet resultSet = db_Statement.executeQuery(query);
                while (resultSet.next()) {
                    candleValues.add(getDb_CandleResult(resultSet));
                }
            } catch (SQLException e) {
                if (googleAuthProxyProcess.isAlive()) {
                    googleAuthProxyProcess.destroy();
                }
                throw new RuntimeException(e);
            }
        }
        return candleValues;
    }

    private List<String> getDb_CandleResult(ResultSet resultSet) throws SQLException {
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

    //must be in the order of ticker, timestamp, open, high, low, close, previous_close, volume!
    // I understand that this is poor design and perhaps later I can remedy it somehow, I hope
    // Outer map key value is Timeframe and inner map key is ticker name
    public synchronized void updateCandleDatabaseTable(String timeFrame, String security, List<List<String>> suppliedCandleValues) {
        if (!googleAuthProxyProcess.isAlive()) {
            System.out.println("GOOGLE AUTH PROXY PROCESS NOT RUNNING!");
            System.out.println("CLOUD DATA NOT UPDATED!");
        } else {
            LocalDateTime earliestDate = LocalDateTime.parse(suppliedCandleValues.getFirst().get(1));
            LocalDateTime latestDate = LocalDateTime.parse(suppliedCandleValues.getLast().get(1));
            List<LocalDateTime> currentCandleDatabaseTimeStamps = retrieveCandleData(timeFrame, security, earliestDate, latestDate).stream().map(tableRow -> LocalDateTime.parse(tableRow.get(1))).toList();
            List<List<String>> filteredSuppliedCandleValues = suppliedCandleValues.stream()
                    .filter(candleValues -> {

                        LocalDateTime givenTimestamp = LocalDateTime.parse(candleValues.get(1));

                        int test = Collections.binarySearch(currentCandleDatabaseTimeStamps, givenTimestamp);
                        return test == -1;
                    }).toList();

            System.out.println();

        }


    }
}
