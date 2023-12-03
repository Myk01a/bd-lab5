import java.sql.*;

public class ResultSetManager {
    private final Connection connection;
    public ResultSetManager(Connection connection) {
        this.connection = connection;
    }
    public void displayFirstTenRecords() {
        String query = "SELECT * FROM titles ORDER BY pubdate ASC LIMIT 10";
        try (PreparedStatement statement = connection.prepareStatement(query);
             ResultSet resultSet = statement.executeQuery()) {
            displayResultSet(resultSet);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void displayInReverseOrderAndIncreasePrice() {
        String query = "SELECT * FROM titles ORDER BY pubdate DESC LIMIT 10";
        try (PreparedStatement statement = connection.prepareStatement(query, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
             ResultSet resultSet = statement.executeQuery()) {
            resultSet.last();
            do {
                double currentPrice = resultSet.getDouble("price");
                double newPrice = (Math.round(currentPrice * 1.05 * 100.0)) / 100.0;
                resultSet.updateDouble("price", newPrice);
                resultSet.updateRow();
            } while (resultSet.previous());
            displayResultSet(resultSet);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    private void displayResultSet(ResultSet resultSet) throws SQLException {
        while (resultSet.next()) {
            String titleId = resultSet.getString("title_id");
            String title = resultSet.getString("title");
            String type = resultSet.getString("type");
            String pubId = resultSet.getString("pub_id");
            double price = resultSet.getDouble("price");
            double advance = resultSet.getDouble("advance");
            double royalty = resultSet.getDouble("royalty");
            int ytdSales = resultSet.getInt("ytd_sales");
//            String notes = resultSet.getString("notes");
            Date pubDate = resultSet.getDate("pubdate");

            System.out.println("Title ID: " + titleId);
            System.out.println("Title: " + title);
            System.out.println("Type: " + type);
            System.out.println("Pub ID: " + pubId);
            System.out.println("Price: " + price);
            System.out.println("Advance: " + advance);
            System.out.println("Royalty: " + royalty);
            System.out.println("YTD Sales: " + ytdSales);
//            System.out.println("Notes: " + notes);
            System.out.println("Pub Date: " + pubDate);
            System.out.println("--------------------");
        }
    }

    public void addNewTitle(String newIdTitle, String newTitle, String newType, String newPubId, Double newPrice,
                            Double newAdvance, Double newRoyalty, Double newYtdSales, String newNotes) {
        String insertQuery = "SELECT * FROM titles WHERE 1=0";
        try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery,
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
             ResultSet resultSet = preparedStatement.executeQuery()) {
            resultSet.moveToInsertRow();
            resultSet.updateString("title_id", newIdTitle);
            resultSet.updateString("title", newTitle);
            resultSet.updateString("type", newType);
            resultSet.updateString("pub_id", newPubId);
            resultSet.updateObject("price", newPrice);
            resultSet.updateObject("advance", newAdvance);
            resultSet.updateObject("royalty", newRoyalty);
            resultSet.updateObject("ytd_sales", newYtdSales);
            resultSet.updateString("notes", newNotes);
            java.util.Date currentDate = new java.util.Date();
            resultSet.updateDate("pubdate", new Date(currentDate.getTime()));
            resultSet.insertRow();
            System.out.println("New title added successfully.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


}

