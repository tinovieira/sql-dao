import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Improve the SQL or code in the methods when necessary
 * Explain each one of the improvements developed
 * It is not necessary that the developed code works
 */
public class TestSqlDao {

    private static TestSqlDao instance = new TestSqlDao();

    private TestSqlDao() {

    }

    private static TestSqlDao getInstance() {
        return instance;
    }

    /**
     * Obtains the ID of the last order for each user
     */
    public Map<Long, Long> getMaxUserOrderId(long idStore) throws SQLException {
        // we do no need synchronized map, since we create a new instance from it in the method
        // we should use cache if we need to persist the result between calls
        final Map<Long, Long> maxOrderUser = new HashMap<>();

        // we should use dbms capabilities to reduce the returned result set
        // note: here we could aggregate also for date column from orders table if the id of the order is not the last order
        final String query = String.format(
                "SELECT ID_USER, MAX(ID_ORDER) as ID_ORDER " +
                        "FROM ORDERS " +
                        "WHERE ID_STORE = %d " +
                        "GROUP_BY ID_USER", idStore);

        final Connection connection = getConnection();

        // we should release resources after finished, we can use the new try-with-resources to do it so
        try (ResultSet rs = connection.prepareStatement(query).executeQuery()) {
            // ids are long not int
            while (rs.next()) {
                long idUser = rs.getLong("ID_USER");
                long idOrder = rs.getLong("ID_ORDER");
                maxOrderUser.put(idUser, idOrder);
            }
        }

        return maxOrderUser;
    }

    /**
     * Copies all the results from one user to another
     */
    public void copyUserOrders(long idUserSource, long idUserTarget) throws SQLException {
        // we should use a insert from select sql and execute the update, to reduce the number of calls to the dbms
        final String query = String.format(
                "INSERT INTO ORDERS(ID_USER, DATE, TOTAL, SUB_TOTAL, ID_STORE) " +
                        "SELECT %d, DATE, TOTAL, SUB_TOTAL, ID_STORE " +
                        "FROM ORDERS " +
                        "WHERE ID_USER = %d",
                idUserTarget,
                idUserSource
        );

        final Connection connection = getConnection();
        connection.setAutoCommit(false);
        // we should use try-with-resources here also
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.executeUpdate();
        }
        connection.commit();
    }

    /**
     * Obtains the user and order data with the id of the most expensive order of the store
     */
    public UserOrderDTO getUserMaxOrder(long idStore) throws SQLException {
        UserOrderDTO userOrder = null;

        // we should perform single query to get tje most expensive order from the store
        final String query = String.format(
                "SELECT U.ID_USER, U.NAME, U.ADDRESS, O.ID_ORDER, MAX(O.TOTAL) AS TOTAL " +
                        "FROM ORDERS AS O INNER JOIN USERS AS U ON O.ID_USER = U.ID_USER " +
                        "WHERE ID_STORE = %d " +
                        "GROUP BY U.ID_USER, U.NAME, U.ADDRESS, O.ID_ORDER",
                idStore);
        final Connection connection = getConnection();

        // we should use try-with-resources
        try (ResultSet rs = connection.prepareStatement(query).executeQuery()) {
            // this is a single query result
            if (rs.next()) {
                userOrder = new UserOrderDTO(
                        rs.getLong("ID_USER"),
                        rs.getString("NAME"),
                        rs.getString("ADDRESS"),
                        rs.getLong("ID_ORDER"),
                        rs.getDouble("TOTAL")
                );
            }
        }

        return userOrder;
    }

    private Connection getConnection() throws SQLException {
        String user = "postgres";
        String pass = "postgres";
        return DriverManager.getConnection("jdbc:postgresql://localhost/online?user=" + user + "&password=" + pass);
    }

    // we should define dto to transport data from dao to service
    // if we need to add more parameters we should use builder pattern
    static class UserOrderDTO {
        private long userId;
        private String name;
        private String address;
        private long orderId;
        private Double orderTotal;

        public UserOrderDTO(long userId, String name, String address, long orderId, Double orderTotal) {
            this.userId = userId;
            this.name = name;
            this.address = address;
            this.orderId = orderId;
            this.orderTotal = orderTotal;
        }

        public long getUserId() {
            return userId;
        }

        public String getName() {
            return name;
        }

        public String getAddress() {
            return address;
        }

        public long getOrderId() {
            return orderId;
        }

        public Double getOrderTotal() {
            return orderTotal;
        }
    }

}
