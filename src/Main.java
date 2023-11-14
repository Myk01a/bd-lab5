import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        DBConnectionManager connectionManager = new DBConnectionManager();

        int choice;
        boolean isConnected = false;
        Connection connection = null;

        while (true) {
            printMenu();
            choice = scanner.nextInt();
            scanner.nextLine();
            File folder = new File("src/lib/bookPublishingDB");
            File[] files = folder.listFiles((dir, name) -> name.toLowerCase().endsWith(".csv"));

            switch (choice) {
                case 1:
                    if (!isConnected) {
                        try {
                            connection = connectionManager.openConnection();
                            isConnected = true;
                            System.out.println("Connected to the database.");
                        } catch (SQLException e) {
                            System.out.println("Error connecting to the database: " + e.getMessage());
                        }
                    } else {
                        System.out.println("Already connected to the database.");
                    }
                    break;

                case 2:
                    if (isConnected) {
                        for (File file : files) {
                            String tableName = file.getName().replace(".csv", "");
                            List<String[]> csvData = null;
                            try {
                                csvData = connectionManager.readCSVFile(file.getAbsolutePath());
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                            connectionManager.createTables();
                            //                                connectionManager.createTableFromCSV(connection, tableName, csvData);
                        }
                        System.out.println("Tables created.");
                        connectionManager.getRecordCount(connection);
                    } else {
                        System.out.println("Not connected to the database. Please connect first.");
                    }
                    break;

                case 3:
                    if (isConnected) {
                        assert files != null;
                        for (File file : files) {
                            String tableName = file.getName().replace(".csv", "");
                            List<String[]> csvData = null;
                            try {
                                csvData = connectionManager.readCSVFile(file.getAbsolutePath());
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }

                            // Insert data into the table
                            try {
                                connectionManager.insertDataIntoTable(connection, tableName, csvData);
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        System.out.println("Insert data option selected.");
                    } else {
                        System.out.println("Not connected to the database. Please connect first.");
                    }
                    connectionManager.getRecordCount(connection);
                    break;

                case 4:
                    if (isConnected) {
                        System.out.print("Enter table name to delete data( authors, publishers, titles, stores, titleauthor, sales): ");
                        String tableToDelete = scanner.nextLine();
                        connectionManager.deleteAllDataFromTable(connection, tableToDelete);
                        System.out.println("Data deleted from table: " + tableToDelete);
                    } else {
                        System.out.println("Not connected to the database. Please connect first.");
                    }
                    connectionManager.getRecordCount(connection);
                    break;

                case 6:
                    if (isConnected) {
                        System.out.print("Enter author's first name: ");
                        String firstName = scanner.nextLine();
                        System.out.print("Enter author's last name: ");
                        String lastName = scanner.nextLine();
                        connectionManager.searchBooksByAuthor(connection, firstName, lastName);
                    } else {
                        System.out.println("Not connected to the database. Please connect first.");
                    }
                    break;

                case 7:
                    if (isConnected) {
                        System.out.print("Enter the number of books (N): ");
                        int N = scanner.nextInt();
                        connectionManager.searchAuthorsWithMoreThanNBooks(connection, N);
                    } else {
                        System.out.println("Not connected to the database. Please connect first.");
                    }
                    break;
                case 8:
                    if (isConnected) {
                        System.out.print("Enter author's first name: ");
                        String firstName = scanner.nextLine();
                        System.out.print("Enter author's last name: ");
                        String lastName = scanner.nextLine();
                        System.out.print("Enter new phone number: ");
                        String newPhone = scanner.nextLine();
                        System.out.print("Enter new address: ");
                        String newAddress = scanner.nextLine();
                        System.out.print("Enter new city: ");
                        String newCity = scanner.nextLine();
                        connectionManager.updateAuthorInfo(connection, firstName, lastName, newPhone, newAddress, newCity);
                    } else {
                        System.out.println("Not connected to the database. Please connect first.");
                    }
                    break;


                case 9:
                    if (isConnected) {
                        connectionManager.dropTables();
                        System.out.println("Tables dropped.");
                    } else {
                        System.out.println("Not connected to the database. Please connect first.");
                    }
                    break;

                case 0:
                    if (isConnected) {
                        try {
                            connection.close();
                        } catch (SQLException e) {
                            System.out.println("Error closing the database connection: " + e.getMessage());
                        }
                    }
                    System.out.println("Exiting the program. Goodbye!");
                    System.exit(0);
                    break;

                default:
                    System.out.println("Invalid choice. Please select a valid option.");
            }
        }
    }

    private static void printMenu() {
        System.out.println("Menu:");
        System.out.println("1 - Connect to the database");
        System.out.println("2 - Create tables");
        System.out.println("3 - Insert data");
        System.out.println("4 - Delete data in the table");
        System.out.println("6 - Search Books by Author");
        System.out.println("7 - Search Authors with more than N Books");
        System.out.println("8 - Update Author Info");
        System.out.println("9 - Drop tables");
        System.out.println("0 - Exit");
        System.out.print("Enter your choice: ");
    }
}



