package br.com.cursokafka.ecommerce;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.cursokafka.ecommerce.consumer.ConsumerService;
import br.com.cursokafka.ecommerce.consumer.ServiceRunner;
import br.com.cursokafka.ecommerce.database.LocalDatabase;
import br.com.cursokafka.ecommerce.dispatcher.KafkaDispatcher;

public class FraudDetectorService implements ConsumerService<Order> {
	
	private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();
	private final LocalDatabase database;
	
	public FraudDetectorService() {
		this.database = new LocalDatabase("frauds_database");
		this.database.createIfNotExists("create table Orders ("
				+ "uuid varchar(200) primary key,"
				+ "is_fraud boolean)");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String[] args) {
		new ServiceRunner(FraudDetectorService::new).start(1);
	}
	
	public void parse(ConsumerRecord<String, Message<Order>> record) throws InterruptedException, ExecutionException, SQLException {
		var message = record.value();
		
		System.out.println("-----------------------------------------");
		System.out.println("Processing new order, checking for fraud ");
		System.out.println(record.key());
		System.out.println(record.value());
		System.out.println(record.partition());
		System.out.println(record.offset());
		var order = message.getPayload();
		
		if(wasProcessed(order)) {
			System.out.println("Order " + order.getOrderId() + " has already been processed!");
			return;
		}
		
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		if(isFraud(order)) {
			database.update("insert into Orders (uuid, is_fraud) values(?, true)", order.getOrderId());
			System.out.println("Order is a fraud");
			orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", 
					order.getEmail(), 
					order, 
					message.getId().continueWith(FraudDetectorService.class.getSimpleName()));
		} else {
			database.update("insert into Orders (uuid, is_fraud) values(?, false)", order.getOrderId());
			System.out.println("Approved: " + order);
			orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", 
					order.getEmail(), 
					order, 
					message.getId().continueWith(FraudDetectorService.class.getSimpleName()));
		}
		
	}

	private boolean wasProcessed(Order order) throws SQLException {
		var results = database.query("select uuid from Orders where uuid = ? limit 1", order.getOrderId());
		return results.next();
	}

	private boolean isFraud(Order order) {
		return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
	}

	@Override
	public String getConsumerGroup() {
		return FraudDetectorService.class.getSimpleName();
	}

	@Override
	public String getTopic() {
		return "ECOMMERCE_NEW_ORDER";
	}

}