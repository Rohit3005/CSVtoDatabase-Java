package org.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FilterReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import static java.lang.Integer.parseInt;

public class CSVtoDatabase {
    public static void main(String[] args) {
        String JDBCUrl = "jdbc:mysql://localhost:3306/csvtodb";
        String userName = "root";
        String password = "root";

        String filePath = "C:\\Users\\Rohit\\OneDrive\\Desktop\\data.csv";

        int batchSize = 20;

        Connection connection = null;

        try {
            connection = DriverManager.getConnection(JDBCUrl, userName, password);
            connection.setAutoCommit(false);

            String sql = "insert into employee(empId, empName, empAddress, empSalary) values(?,?,?,?)";

            PreparedStatement statement = connection.prepareStatement(sql);

            BufferedReader reader = new BufferedReader(new FileReader(filePath));

            String lineText = null;
            int count = 0;

            reader.readLine();
            while ((lineText = reader.readLine()) != null) {
                String[] data = lineText.split(",");

                String empId = data[0];
                String empName = data[1];
                String empAddress = data[2];
                String empSalary = data[3];

                statement.setInt(1, parseInt(empId));
                statement.setString(2, empName);
                statement.setString(3, empAddress);
                statement.setDouble(4, Double.parseDouble(empSalary));

                statement.addBatch();
                if (count % batchSize == 0) {
                    statement.executeBatch();
                }


            }
            reader.close();
            statement.executeBatch();
            connection.commit();
            connection.close();
            System.out.println("Data inserted");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
