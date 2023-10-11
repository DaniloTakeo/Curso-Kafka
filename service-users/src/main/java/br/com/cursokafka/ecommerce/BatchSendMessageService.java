package br.com.cursokafka.ecommerce;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.cursokafka.ecommerce.consumer.KafkaService;
import br.com.cursokafka.ecommerce.dispatcher.KafkaDispatcher;


public class BatchSendMessageService {

	private Connection connection;
	private final KafkaDispatcher<User> userDispatcher = new KafkaDispatcher<User>();

	public BatchSendMessageService() throws SQLException {
		String url = "jdbc:sqlite:target/users_database.db";
		
		try {
			this.connection = DriverManager.getConnection(url);
			connection.createStatement().execute("create table Users(" + 
					"uuid varchar(200) primary key, " +
					"email varchar(200))");			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws SQLException, ExecutionException, InterruptedException {
		var batchService = new BatchSendMessageService();
		try(var kafkaService = new KafkaService<String>(BatchSendMessageService.class.getSimpleName(),
				"ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS",
				batchService::parse,
				Map.of())) {
			kafkaService.run();
		}
	}
	
	private void parse(ConsumerRecord<String, Message<String>> record) throws InterruptedException, ExecutionException, SQLException {
		System.out.println(record);
		var message = record.value();
		
		System.out.println("-----------------------------------------");
		System.out.println("Processing new batch ");
		System.out.println("Topic: " + message.getPayload());
		
		for (User user: getAllUser()) {
			userDispatcher.sendAsync(message.getPayload(), 
					user.getUuid(), 
					user, 
					message.getId().continueWith(BatchSendMessageService.class.getSimpleName()));
			
			System.out.println("Enviei para o user " + user);
		}
		
	}

	private List<User> getAllUser() throws SQLException {
		var results = connection.prepareStatement("select uuid from Users").executeQuery();
		List<User> users = new ArrayList<>();
		while(results.next()) {
			users.add(new User(results.getString(1)));
		}
		
		return users;
	}
}
