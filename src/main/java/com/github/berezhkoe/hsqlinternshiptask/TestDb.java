package com.github.berezhkoe.hsqlinternshiptask;

import com.opencsv.CSVReader;

import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class TestDb {
    private final Connection connection;

    public TestDb(String db_name) throws Exception {
        Class.forName("org.hsqldb.jdbcDriver");
        connection = DriverManager.getConnection("jdbc:hsqldb:" + db_name,"sa","");
    }

    public void shutdown() throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("SHUTDOWN");
        connection.close();
    }

    public synchronized void query(String expression) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(expression);

        dump(rs);
        statement.close();
    }

    public synchronized void update(String expression) throws SQLException {
        Statement statement = connection.createStatement();

        if (statement.executeUpdate(expression) == -1) {
            System.out.println("db error : " + expression);
        }
        statement.close();
    }

    public static void dump(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        while (rs.next()) {
            for (int i = 0; i < meta.getColumnCount(); i++) {
                Object o = rs.getObject(i + 1);
                System.out.print(o + " ");
            }
            System.out.print("\n");
        }
    }

    public static void main(String[] args) {
        String DATA_TYPE = "VARCHAR(256)";
        String fileName = args[0];

        try (CSVReader reader = new CSVReader(new FileReader(fileName))) {
            final String name = fileName.split("\\.")[0];
            TestDb db = new TestDb(name + "_db");

            String[] lineInArray = reader.readNext();
            if (lineInArray != null) {
                StringBuilder createTableCommand = new StringBuilder();
                createTableCommand.append("CREATE TABLE IF NOT EXISTS ").append(name).append(" (");
                for (int i = 0; i < lineInArray.length; i++) {
                    createTableCommand.append(lineInArray[i]).append(" ").append(DATA_TYPE);
                    if (i < lineInArray.length - 1) {
                        createTableCommand.append(", ");
                    }
                }
                createTableCommand.append(");");
                db.update(createTableCommand.toString());
            }

            while ((lineInArray = reader.readNext()) != null) {
                StringBuilder insertIntoCommand = new StringBuilder();
                insertIntoCommand.append("INSERT INTO ").append(name).append(" VALUES (");
                for (int i = 0; i < lineInArray.length; i++) {
                    insertIntoCommand.append("'").append(lineInArray[i]).append("'");
                    if (i < lineInArray.length - 1) {
                        insertIntoCommand.append(", ");
                    }
                }
                insertIntoCommand.append(");");
                db.update(insertIntoCommand.toString());

            }

            db.query("SELECT * FROM " + name);

            db.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
