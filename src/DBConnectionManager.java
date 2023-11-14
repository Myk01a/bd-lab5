import com.opencsv.CSVReader;
import org.sqlite.SQLiteConfig;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.Collections;
import java.util.List;

public class DBConnectionManager {
    private static final String JDBC_URL_IN_FILE = "jdbc:sqlite:Lab1DB.db";

    public Connection openConnection() throws SQLException {
        SQLiteConfig config = new SQLiteConfig();

        config.enforceForeignKeys(true);
        Connection connection = DriverManager.getConnection(JDBC_URL_IN_FILE,
                config.toProperties());
        return connection;
    }

    public void createTables() {
        final String createAuthorsTableString =
                "create table if not exists authors " +
                        "(au_id TEXT PRIMARY KEY , " +
                        "au_lname TEXT NOT NULL, " +
                        "au_fname TEXT NOT NULL, " +
                        "phone TEXT NOT NULL, " +
                        "address TEXT NOT NULL, " +
                        "city TEXT NOT NULL, " +
                        "state TEXT NOT NULL, " +
                        "zip TEXT NOT NULL, " +
                        "contract TEXT NOT NULL)";

        final String createPublishersTableString =
                "create table if not exists publishers " +
                        "(pub_id TEXT NOT NULL, " +
                        "pub_name TEXT NOT NULL, " +
                        "city TEXT NOT NULL, " +
                        "state TEXT NOT NULL, " +
                        "country TEXT NOT NULL, " +
                        "PRIMARY KEY (pub_id))";

//        title_id,title,type,pub_id,price,advance,royalty,ytd_sales,notes,pubdate
        final String createTitlesTableString =
                "create table if not exists titles " +
                        "(title_id TEXT PRIMARY KEY , " +
                        "title TEXT NOT NULL, " +
                        "type TEXT NOT NULL, " +
                        "pub_id TEXT, " +
                        "price TEXT NOT NULL, " +
                        "advance TEXT NOT NULL, " +
                        "royalty TEXT NOT NULL, " +
                        "ytd_sales TEXT NOT NULL, " +
                        "notes TEXT NOT NULL, " +
                        "pubdate TEXT NOT NULL, " +
                        "FOREIGN KEY (pub_id) REFERENCES publishers (pub_id) ON DELETE CASCADE)";
//        stor_id,stor_name,stor_address,city,state,zip
        final String createStoresTableString =
                "create table if not exists stores " +
                        "(stor_id TEXT PRIMARY KEY , " +
                        "stor_name TEXT NOT NULL, " +
                        "stor_address TEXT NOT NULL, " +
                        "city TEXT NOT NULL, " +
                        "state TEXT NOT NULL, " +
                        "zip TEXT NOT NULL)";
//        au_id,title_id,au_ord,royaltyper
        final String createTitleautorTableString =
                "create table if not exists titleauthor " +
                        "(au_id  TEXT NOT NULL," +
                        "title_id TEXT NOT NULL," +
                        "au_ord TEXT NOT NULL, " +
                        "royaltyper TEXT NOT NULL, " +
                        "FOREIGN KEY (au_id) REFERENCES authors (au_id) ON DELETE CASCADE, " +
                        "FOREIGN KEY (title_id) REFERENCES titles (title_id) ON DELETE CASCADE)";

//        stor_id,ord_num,ord_date,qty,payterms,title_id
        final String createSalesTableString =
                "create table if not exists sales " +
                        "(stor_id  TEXT NOT NULL ," +
                        "order_num TEXT KEY, " +
                        "order_date TEXT NOT NULL, " +
                        "qty TEXT NOT NULL, " +
                        "payterms TEXT NOT NULL, " +
                        "title_id TEXT NOT NULL, " +
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
//    public void createTables() {
////        au_id,au_lname,au_fname,phone,address,city,state,zip,contract
//        final String createAuthorsTableString =
//                "create table if not exists authors " +
//                        "(au_id INTEGER PRIMARY KEY , " +
//                        "au_lname varchar(40) NOT NULL, " +
//                        "au_fname varchar(40) NOT NULL, " +
//                        "phone varchar(40) NOT NULL, " +
//                        "address varchar(40) NOT NULL, " +
//                        "city varchar(20) NOT NULL, " +
//                        "state char(2) NOT NULL, " +
//                        "zip char(5) NOT NULL, " +
//                        "contract bit NOT NULL)";
//
//        final String createPublishersTableString =
//                "create table if not exists publishers " +
//                        "(pub_id char(4) NOT NULL, " +
//                        "pub_name varchar(40) NOT NULL, " +
//                        "city varchar(20) NOT NULL, " +
//                        "state char(2) NOT NULL, " +
//                        "country varchar(30) NOT NULL, " +
//                        "PRIMARY KEY (pub_id))";
//
//        final String createTitlesTableString =
//                "create table if not exists titles " +
//                        "(title_id INTEGER PRIMARY KEY AUTOINCREMENT, " +
//                        "title varchar(80) NOT NULL, " +
//                        "type char(12) NOT NULL, " +
//                        "pub_id char(4) NOT NULL, " +
//                        "price REAL NOT NULL, " +
//                        "advance REAL NOT NULL, " +
//                        "royalty integer NOT NULL, " +
//                        "ytd_sales integer NOT NULL, " +
//                        "notes varchar(200) NOT NULL, " +
//                        "pubdate TEXT NOT NULL, " +
//                        "FOREIGN KEY (pub_id) REFERENCES publishers (pub_id))";
//
//        final String createStoresTableString =
//                "create table if not exists stores " +
//                        "(stor_id INTEGER PRIMARY KEY AUTOINCREMENT, " +
//                        "stor_name varchar(40) NOT NULL, " +
//                        "stor_address varchar(40) NOT NULL, " +
//                        "city varchar(20) NOT NULL, " +
//                        "state char(2) NOT NULL, " +
//                        "zip char(2) NOT NULL)";
//
//        final String createTitleautorTableString =
//                "create table if not exists titleautor " +
//                        "(au_ord int NOT NULL, " +
//                        "royaltyper int NOT NULL, " +
//                        "pub_id char(4) NOT NULL, " +
//                        "price REAL NOT NULL, " +
//                        "advance REAL NOT NULL, " +
//                        "royalty integer NOT NULL, " +
//                        "au_id  integer NOT NULL, " +
//                        "title_id integer NOT NULL, " +
//                        "FOREIGN KEY (au_id) REFERENCES authors (au_id), " +
//                        "FOREIGN KEY (title_id) REFERENCES titles (title_id))";
//
//        final String createSalesTableString =
//                "create table if not exists sales " +
//                        "(order_num varchar(20) PRIMARY KEY, " +
//                        "order_date text NOT NULL, " +
//                        "qty int NOT NULL, " +
//                        "payterms text NOT NULL, " +
//                        "stor_id  char(4) NOT NULL, " +
//                        "title_id integer NOT NULL, " +
//                        "FOREIGN KEY (stor_id) REFERENCES stores (stor_id), " +
//                        "FOREIGN KEY (title_id) REFERENCES titles (title_id))";
//
//        DBConnectionManager connectionManager = new DBConnectionManager();
//
//        try (Connection connection = connectionManager.openConnection();
//             Statement statement = connection.createStatement()) {
//            System.out.println("Creating tables");
//            statement.executeUpdate(createAuthorsTableString);
//            statement.executeUpdate(createPublishersTableString);
//            statement.executeUpdate(createTitlesTableString);
//            statement.executeUpdate(createStoresTableString);
//            statement.executeUpdate(createTitleautorTableString);
//            statement.executeUpdate(createSalesTableString);
//            System.out.println("Tables were created, please see report");
//        } catch (SQLException sqlException) {
//            System.out.println("During execution of Create statement, the following SQL error occurred: " + sqlException.getMessage());
//        }
//    }

    public void dropTables() {
        DBConnectionManager connectionManager = new DBConnectionManager();
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

            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException sqlException) {
                    System.out.println("Problem occurred during closing statement");
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException sqlException) {
                    System.out.println("Problem occurred during closing connection");
                }
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

    public void createTableFromCSV(Connection connection, String tableName, List<String[]> csvData) throws SQLException {
        StringBuilder createTableSQL = new StringBuilder("CREATE TABLE IF NOT EXISTS " + tableName + " (");

        String[] columnNames = csvData.get(0);

        for (String columnName : columnNames) {
            createTableSQL.append(columnName).append(" TEXT, ");
        }

        createTableSQL.setLength(createTableSQL.length() - 2);
        createTableSQL.append(")");

        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(createTableSQL.toString());
        }
    }

    public void insertDataIntoTable(Connection connection, String tableName, List<String[]> csvData) throws SQLException {
        try (Statement pragmaStatement = connection.createStatement()) {
            pragmaStatement.execute("PRAGMA foreign_keys = OFF;");
        } catch (SQLException sqlException) {
            System.out.println("Error disabling foreign keys: " + sqlException.getMessage());
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
            try (Statement pragmaStatement = connection.createStatement()) {
                pragmaStatement.execute("PRAGMA foreign_keys = ON;");
            } catch (SQLException sqlException) {
                System.out.println("Error enabling foreign keys: " + sqlException.getMessage());
            }
        } catch (SQLException sqlException) {
            System.out.println("Error inserting data into the table: " + sqlException.getMessage());
        }
    }
    public void getRecordCount(Connection connection) {
        String[] tableNames = {"authors", "publishers", "titles", "stores", "titleauthor", "sales"};

        try (Statement statement = connection.createStatement()) {
            for (String tableName : tableNames) {
                String countQuery = "SELECT COUNT(*) AS count FROM " + tableName;
                ResultSet resultSet = statement.executeQuery(countQuery);
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
                authorsStatement.setInt(1, N-1);
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
