import com.opencsv.CSVReader;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class DBConnectionManager {
//    private static final String JDBC_URL_IN_FILE = "jdbc:mysql://localhost:3306/testdb";
//    private static final String JDBC_URL_IN_FILE = "jdbc:sqlite:Lab1DB.db";


    private static final String JDBC_URL_MYSQL = "jdbc:mysql://localhost:3306/testdb";
    private static final String USERNAME = System.getenv("DB_USERNAME");
    private static final String PASSWORD = System.getenv("DB_PASSWORD");

    public Connection openConnection() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", USERNAME);
        properties.setProperty("password", PASSWORD);

        Connection connection = DriverManager.getConnection(JDBC_URL_MYSQL, properties);
        return connection;
    }

    public void createTables() {
        final String createAuthorsTableString =
                "CREATE TABLE IF NOT EXISTS authors " +
                        "(au_id VARCHAR(255) PRIMARY KEY, " +
                        "au_lname VARCHAR(255) NOT NULL, " +
                        "au_fname VARCHAR(255) NOT NULL, " +
                        "phone VARCHAR(255) NOT NULL, " +
                        "address VARCHAR(255) NOT NULL, " +
                        "city VARCHAR(255) NOT NULL, " +
                        "state VARCHAR(255) NOT NULL, " +
                        "zip VARCHAR(255) NOT NULL, " +
                        "contract VARCHAR(255) NOT NULL)";

        final String createPublishersTableString =
                "CREATE TABLE IF NOT EXISTS publishers " +
                        "(pub_id VARCHAR(255) NOT NULL, " +
                        "pub_name VARCHAR(255) NOT NULL, " +
                        "city VARCHAR(255) NOT NULL, " +
                        "state VARCHAR(255) NOT NULL, " +
                        "country VARCHAR(255) NOT NULL, " +
                        "PRIMARY KEY (pub_id))";

        final String createTitlesTableString =
                "CREATE TABLE IF NOT EXISTS titles " +
                        "(title_id VARCHAR(255) PRIMARY KEY , " +
                        "title VARCHAR(255) NOT NULL, " +
                        "type VARCHAR(255) NOT NULL, " +
                        "pub_id VARCHAR(255), " +
                        "price DECIMAL(10, 2) DEFAULT NULL," +
                        "advance DECIMAL(10, 2) DEFAULT NULL, " +
                        "royalty DECIMAL(5, 2) DEFAULT NULL, " +
                        "ytd_sales INT DEFAULT NULL, " +
                        "notes TEXT DEFAULT NULL," +
                        "pubdate DATE, " +
                        "FOREIGN KEY (pub_id) REFERENCES publishers (pub_id) ON DELETE CASCADE)";

        final String createStoresTableString =
                "CREATE TABLE IF NOT EXISTS stores " +
                        "(stor_id VARCHAR(255) PRIMARY KEY , " +
                        "stor_name VARCHAR(255) NOT NULL, " +
                        "stor_address VARCHAR(255) NOT NULL, " +
                        "city VARCHAR(255) NOT NULL, " +
                        "state VARCHAR(255) NOT NULL, " +
                        "zip VARCHAR(255) NOT NULL)";

        final String createTitleautorTableString =
                "CREATE TABLE IF NOT EXISTS titleauthor " +
                        "(au_id  VARCHAR(255) NOT NULL," +
                        "title_id VARCHAR(255) NOT NULL," +
                        "au_ord VARCHAR(255) NOT NULL, " +
                        "royaltyper VARCHAR(255) NOT NULL, " +
                        "FOREIGN KEY (au_id) REFERENCES authors (au_id) ON DELETE CASCADE, " +
                        "FOREIGN KEY (title_id) REFERENCES titles (title_id) ON DELETE CASCADE)";

        final String createSalesTableString =
                "CREATE TABLE IF NOT EXISTS sales " +
                        "(stor_id VARCHAR(255) NOT NULL ," +
                        "order_num VARCHAR(255) PRIMARY KEY, " +
                        "order_date DATE NOT NULL, " +
                        "qty INT NOT NULL, " +
                        "payterms VARCHAR(255) NOT NULL, " +
                        "title_id VARCHAR(255) NOT NULL, " +
                        "FOREIGN KEY (stor_id) REFERENCES stores (stor_id) ON DELETE CASCADE, " +
                        "FOREIGN KEY (title_id) REFERENCES titles (title_id) ON DELETE CASCADE)";


        DBConnectionManager connectionManager = new DBConnectionManager();

        try (Connection connection = connectionManager.openConnection();
             Statement statement = connection.createStatement()) {
            System.out.println("Creating tables");
            statement.executeUpdate(createAuthorsTableString);
            statement.executeUpdate(createPublishersTableString);
            statement.executeUpdate(createStoresTableString);
            statement.executeUpdate(createTitlesTableString);
            statement.executeUpdate(createTitleautorTableString);
            statement.executeUpdate(createSalesTableString);
            System.out.println("Tables were created");
        } catch (SQLException sqlException) {
            System.out.println("During execution of Create statement, the following SQL error occurred: " + sqlException.getMessage());
        }
    }

    public void dropTables(DBConnectionManager connectionManager) {
        Connection connection = null;
        Statement statement = null;

        try {
            connection = connectionManager.openConnection();
            statement = connection.createStatement();
            System.out.println("Dropping tables from db");

            statement.executeUpdate("DROP TABLE IF EXISTS sales");
            statement.executeUpdate("DROP TABLE IF EXISTS titleauthor");
            statement.executeUpdate("DROP TABLE IF EXISTS stores");
            statement.executeUpdate("DROP TABLE IF EXISTS titles");
            statement.executeUpdate("DROP TABLE IF EXISTS publishers");
            statement.executeUpdate("DROP TABLE IF EXISTS authors");
            System.out.println("All tables were successfully dropped");

        } catch (SQLException sqlException) {
            System.out.println("During execution of Drop statement, the following SQL error occurred: "
                    + sqlException.getMessage());
        } finally {
            try {
                if (statement != null) {
                    statement.close();
                }
            } catch (SQLException sqlException) {
                System.out.println("Problem occurred during closing statement");
            }
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException sqlException) {
                System.out.println("Problem occurred during closing connection");
            }
        }
    }

    public List<String[]> readCSVFile(String filePath) throws IOException {
        try {
            CSVReader reader = new CSVReader(new FileReader(filePath));
            List<String[]> lines = reader.readAll();
            return lines;
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public void insertDataIntoTable(Connection connection, String tableName, List<String[]> csvData) throws SQLException {
        try (Statement disableForeignKeyChecks = connection.createStatement()) {
            disableForeignKeyChecks.execute("SET FOREIGN_KEY_CHECKS=0");
        } catch (SQLException disableFkException) {
            System.out.println("Error disabling foreign key checks: " + disableFkException.getMessage());
        }

        String insertSQL = "INSERT INTO " + tableName + " VALUES (" + String.join(", ", Collections.nCopies(csvData.get(0).length, "?")) + ")";

        try (PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {
            for (int i = 1; i < csvData.size(); i++) {
                String[] rowData = csvData.get(i);
                for (int j = 0; j < rowData.length; j++) {
                    preparedStatement.setString(j + 1, rowData[j]);
                }
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
        } catch (SQLException sqlException) {
            System.out.println("Error inserting data into the table: " + sqlException.getMessage());
        } finally {
            try (Statement enableForeignKeyChecks = connection.createStatement()) {
                enableForeignKeyChecks.execute("SET FOREIGN_KEY_CHECKS=1");
            } catch (SQLException enableFkException) {
                System.out.println("Error enabling foreign key checks: " + enableFkException.getMessage());
            }
        }
    }


    public void getRecordCount(Connection connection) {
        String[] tableNames = {"authors", "publishers", "titles", "stores", "titleauthor", "sales"};

        try (Statement statement = connection.createStatement()) {
            for (String tableName : tableNames) {
                String countQuery = "SELECT COUNT(*) AS count FROM " + tableName;
                ResultSet resultSet = statement.executeQuery(countQuery);
                resultSet.next();
                int count = resultSet.getInt("count");

                System.out.println("Table " + tableName + " has " + count + " records.");
            }
        } catch (SQLException e) {
            System.out.println("Error getting record count: " + e.getMessage());
        }
    }


    public void deleteAllDataFromTable(Connection connection, String tableName) {
        try (Statement statement = connection.createStatement()) {
            String deleteQuery = "DELETE FROM " + tableName;
            int rowsAffected = statement.executeUpdate(deleteQuery);
            System.out.println(rowsAffected + " rows deleted from table " + tableName);
        } catch (SQLException e) {
            System.out.println("Error deleting data from table " + tableName + ": " + e.getMessage());
        }
    }

    public void searchBooksByAuthor(Connection connection, String firstName, String lastName) {
        try {
            String authorIdQuery = "SELECT au_id FROM authors WHERE au_fname = ? AND au_lname = ?";
            try (PreparedStatement authorIdStatement = connection.prepareStatement(authorIdQuery)) {
                authorIdStatement.setString(1, firstName);
                authorIdStatement.setString(2, lastName);
                ResultSet authorIdResult = authorIdStatement.executeQuery();

                if (authorIdResult.next()) {
                    String authorId = authorIdResult.getString("au_id");

                    String booksQuery = "SELECT titles.title, authors.au_fname, authors.au_lname, titles.pubdate " +
                            "FROM titles " +
                            "INNER JOIN titleauthor ON titles.title_id = titleauthor.title_id " +
                            "INNER JOIN authors ON titleauthor.au_id = authors.au_id " +
                            "WHERE authors.au_id = ?";

                    try (PreparedStatement booksStatement = connection.prepareStatement(booksQuery)) {
                        booksStatement.setString(1, authorId);
                        ResultSet booksResult = booksStatement.executeQuery();

                        while (booksResult.next()) {
                            String title = booksResult.getString("title");
                            String authorFirstName = booksResult.getString("au_fname");
                            String authorLastName = booksResult.getString("au_lname");
                            String publishDate = booksResult.getString("pubdate");

                            System.out.println("Book: " + title);
                            System.out.println("Author: " + authorFirstName + " " + authorLastName);
                            System.out.println("Published Date: " + publishDate);
                            System.out.println("--------------------");
                        }
                    }
                } else {
                    System.out.println("Author not found");
                }
            }
        } catch (SQLException e) {
            System.out.println("Error searching books by author: " + e.getMessage());
        }
    }

    public void searchAuthorsWithMoreThanNBooks(Connection connection, int N) {
        try {
            String authorsQuery = "SELECT authors.au_fname, authors.au_lname, COUNT(titleauthor.title_id) AS book_count " +
                    "FROM authors " +
                    "LEFT JOIN titleauthor ON authors.au_id = titleauthor.au_id " +
                    "GROUP BY authors.au_id " +
                    "HAVING COUNT(titleauthor.title_id) > ?";

            try (PreparedStatement authorsStatement = connection.prepareStatement(authorsQuery)) {
                authorsStatement.setInt(1, N - 1);
                ResultSet authorsResult = authorsStatement.executeQuery();

                while (authorsResult.next()) {
                    String authorFirstName = authorsResult.getString("au_fname");
                    String authorLastName = authorsResult.getString("au_lname");
                    int bookCount = authorsResult.getInt("book_count");

                    System.out.println("Author: " + authorFirstName + " " + authorLastName);
                    System.out.println("Number of Books: " + bookCount);
                    System.out.println("--------------------");
                }
            }
        } catch (SQLException e) {
            System.out.println("Error searching authors with more than N books: " + e.getMessage());
        }
    }

    public void updateAuthorInfo(Connection connection, String firstName, String lastName, String newPhone, String newAddress, String newCity) {
        try {
            String updateQuery = "UPDATE authors SET phone = ?, address = ?, city = ? WHERE au_fname = ? AND au_lname = ?";

            try (PreparedStatement updateStatement = connection.prepareStatement(updateQuery)) {
                updateStatement.setString(1, newPhone);
                updateStatement.setString(2, newAddress);
                updateStatement.setString(3, newCity);
                updateStatement.setString(4, firstName);
                updateStatement.setString(5, lastName);

                int rowsUpdated = updateStatement.executeUpdate();
                if (rowsUpdated > 0) {
                    System.out.println("Author information updated for " + firstName + " " + lastName);
                } else {
                    System.out.println("Author not found or information remains unchanged.");
                }
            }
        } catch (SQLException e) {
            System.out.println("Error updating author information: " + e.getMessage());
        }
    }


}
