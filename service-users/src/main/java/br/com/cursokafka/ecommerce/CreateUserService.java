package br.com.cursokafka.ecommerce;

import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.Uuid;

import br.com.cursokafka.ecommerce.consumer.ConsumerService;
import br.com.cursokafka.ecommerce.consumer.ServiceRunner;
import br.com.cursokafka.ecommerce.database.LocalDatabase;

public class CreateUserService implements ConsumerService<Order> {
	
	private final LocalDatabase database;

	public CreateUserService() {
		this.database = new LocalDatabase("users_database");
		this.database.createIfNotExists("create table Users ("
				+ "uuid varchar(200) primary key,"
				+ "email varchar(200))");
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String[] args) {
		new ServiceRunner(CreateUserService::new).start(1);
	}
	
	public void parse(ConsumerRecord<String, Message<Order>> record) throws InterruptedException, ExecutionException, SQLException {
		var message = record.value();
		
		System.out.println("-----------------------------------------");
		System.out.println("Processing new order, checking for new user");
		System.out.println(message.getPayload());
		
		var order = message.getPayload();
		if(isNewUser(order.getEmail())) {
			insertNewUser(order.getEmail());
		}
	}

	private void insertNewUser(String email) throws SQLException {
		var uuid = Uuid.randomUuid().toString();
		database.update("insert into Users(uuid, email) values(?, ?)", uuid, email);
		
		System.out.println("Usuário " + uuid + " e email " + email +  " adicionado");
	}

	private boolean isNewUser(String email) throws SQLException {
		var results = database.query("select uuid from Users where email = ? limit 1", email);
		
		return !results.next();
	}

	@Override
	public String getConsumerGroup() {
		return CreateUserService.class.getSimpleName();
	}

	@Override
	public String getTopic() {
		return "ECOMMERCE_NEW_ORDER";
	}

}
