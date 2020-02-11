import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Hashtable;

/**
 * Improve the SQL or code in the methods when necessary
 * Explain each one of the improvements developed
 * It is not necessary that the developed code works
 */
public class TestSqlDao {

	private static TestSqlDao instance = new TestSqlDao();
	private Hashtable<Long, Long> maxOrderUser;
	
	private TestSqlDao() {

	}

	private static TestSqlDao getInstance() {

		return instance;
	}

	/**
	 * Obtains the ID of the last order for each user
	 */
	public Hashtable<Long, Long> getMaxUserOrderId(long idStore) throws SQLException {

		String query = String.format("SELECT ID_ORDER, ID_USER FROM ORDERS WHERE ID_STORE = %s", idStore);
		Connection connection = getConnection();
		PreparedStatement stmt = connection.prepareStatement(query);
		ResultSet rs = stmt.executeQuery();
		maxOrderUser = new Hashtable<Long, Long>();
		
		while (rs.next()) {

			long idOrder = rs.getInt("ID_ORDER");
			long idUser = rs.getInt("ID_USER");
			
			if (!maxOrderUser.containsKey(idUser)) {

				maxOrderUser.put(idUser, idOrder);

			} else if (maxOrderUser.get(idUser) < idOrder ) {

				maxOrderUser.put(idUser, idOrder );
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

	private Connection getConnection() {

		// return JDBC connection
		return null;
	}
}
