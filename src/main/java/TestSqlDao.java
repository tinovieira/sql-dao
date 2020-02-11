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
        Map<Long, Long> maxOrderUser = new HashMap<>();

        // we should use dbms capabilities to reduce the returned result set
        String query = String.format(
                "SELECT ID_USER, MAX(ID_ORDER) as ID_ORDER " +
                        "FROM ORDERS WHERE ID_STORE = %s " +
                        "GROUP_BY ID_USER", idStore);

        Connection connection = getConnection();

        // we should release resources, we can use the new try with resources to do it so
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

        String query = String.format("SELECT DATE, TOTAL, SUBTOTAL, ADDRESS FROM ORDERS WHERE ID_USER = %s", idUserSource);
        Connection connection = getConnection();
        PreparedStatement stmt = connection.prepareStatement(query);
        ResultSet rs = stmt.executeQuery();

        while (rs.next()) {

            String insert = String.format("INSERT INTO ORDERS (DATE, TOTAL, SUBTOTAL, ADDRESS) VALUES (%s, %s, %s, %s)",
                    rs.getTimestamp("DATE"),
                    rs.getDouble("TOTAL"),
                    rs.getDouble("SUBTOTAL"),
                    rs.getString("ADDRESS"));

            Connection connection2 = getConnection();
            connection2.setAutoCommit(false);
            PreparedStatement stmt2 = connection2.prepareStatement(insert);
            stmt2.executeUpdate();
            connection2.commit();
        }
    }

    /**
     * Obtains the user and order data with the id of the most expensive order of the store
     */
    public void getUserMaxOrder(long idStore, long userId, long orderId, String name, String address) throws SQLException {

        String query = String.format("SELECT U.ID_USER, O.ID_ORDER, O.TOTAL, U.NAME, U.ADDRESS FROM ORDERS AS O "
                + "INNER JOIN USERS AS U ON O.ID_USERS = U.ID_USER WHERE O.ID_STORE = %", idStore);
        Connection connection = getConnection();
        PreparedStatement stmt = connection.prepareStatement(query);
        ResultSet rs = stmt.executeQuery();
        double total = 0;

        while (rs.next()) {

            if (rs.getLong("TOTAL") > total) {

                total = rs.getLong("TOTAL");
                userId = rs.getInt("ID_USER");
                orderId = rs.getInt("ID_ORDER");
                name = rs.getString("NAME");
                address = rs.getString("ADDRESS");
            }
        }
    }

    private Connection getConnection() throws SQLException {
        String user = "postgres";
        String pass = "postgres";
        return DriverManager.getConnection("jdbc:postgresql://localhost/online?user=" + user + "&password=" + pass);
    }

}
