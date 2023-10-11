package br.com.cursrokafka.ecommerce;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;

import br.com.cursokafka.ecommerce.database.LocalDatabase;

public class OrdersDatabase implements Closeable {
	
	private final LocalDatabase database;
	
	public OrdersDatabase() {
		this.database = new LocalDatabase("orders_database");
		this.database.createIfNotExists("create table Orders("
				+ "uuid varchahr(200) primary key)");
	}

	public boolean saveNew(Order order) throws SQLException {
		if(wasProcessed(order)) return false;
		
		database.update("insert into Orders(uuid) values(?)", order.getOrderId());
		return true;
	}
	
	private boolean wasProcessed(Order order) throws SQLException {
		var results = database.query("select uuid from Orders where uuid = ? limit 1", order.getOrderId());
		return results.next();
	}

	@Override
	public void close() throws IOException {
		try {
			database.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
