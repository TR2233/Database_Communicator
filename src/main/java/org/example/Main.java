package org.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Main {

    public static void main(String[] args) {
        System.out.println("Start Program");
        List<List<String>> testList = new ArrayList<>();
        testList.add(List.of("MES", LocalDateTime.of(2024, 12, 10, 9, 30).toString(), "1"));
        testList.add(List.of("MES", LocalDateTime.of(2024, 12, 11, 9, 30).toString(), "2"));
        testList.add(List.of("MES", LocalDateTime.of(2024, 12, 12, 9, 30).toString(), "3"));
        testList.add(List.of("MES", LocalDateTime.of(2026, 12, 12, 9, 30).toString(), "4"));
        List<List<String>> localData = getLocalData();
//        System.out.println();
        DB_Communicator db_communicator = DB_Communicator.getInstance();
        db_communicator.connectToDatabase();
//        List<List<String>> stringListMap = db_communicator.retrieveCandleData("FIVE_MINUTE","MES", LocalDateTime.of(2025, 1, 1, 0, 0), LocalDateTime.now());
        db_communicator.updateCandleDatabaseTable("FIVE_MINUTE", "MES", getLocalData());
//        System.out.println();
        db_communicator.disconnectFromDatabase();
    }


    private static List<List<String>> getLocalData() {
        List<List<String>> data = new ArrayList<>();

        try (BufferedReader bufferedReader =
                     new BufferedReader(new FileReader("C:\\Users\\lenovo\\Documents\\Historical_Stock_Data\\Tickers\\MES\\5M\\Formatted_CSV_Files\\final_Historical_Ticker_Data\\MES_2024_12_10_2025_06_27.csv"))) {

            bufferedReader.lines().forEach(line -> {
                if (line.contains(":")) {
//                    data.add(LocalDateTime.parse(line.split(",")[1].replace("\"", "")));
//                    line.split(",").toString();
                    data.add(Arrays.stream(line.replace("\"","").split(",")).toList());
                }
            });

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return data;
    }


}