package org.example;

import java.sql.*;

public class Main {
    static final String JDBC_URL = "jdbc:postgresql://localhost:5432/Historical_Data?currentSchema=public&user=postgres&password=Federer!66";

    public static void main(String[] args) {
        System.out.println("Hello, World!");

        try {
//            Connection connection = DriverManager.getConnection(JDBC_URL);
//            createTable(connection);
            retrieveDatabase();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void retrieveDatabase() throws SQLException {



        String dbUrl = "jdbc:postgresql://localhost:6000/testDataBase";
String dbUser = "postgres";
String dbPassword = "Federer!66";

 try {
            Connection connection1 = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
            System.out.println("Successfully connected to the database!");
            connection1.close();
        } catch (SQLException e) {
            System.err.println("Error connecting to the database: " + e.getMessage());
        }

//        String query = "select \"DATE_TIME\" from \"MES_5MIN\"";
//        try(Statement statement = connection.createStatement()) {
//            ResultSet resultSet = statement.executeQuery(query);
//            while (resultSet.next()) {
//                System.out.println(resultSet.getString("DATE_TIME"));
//            }
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }


    }

    private static void createTable(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        String createTableStatement = "CREATE TABLE TASKS (id SERIAL PRIMARY KEY, name VARCHAR(255))";
        statement.execute(createTableStatement);
        System.out.println("Table created!!!");
    }
}