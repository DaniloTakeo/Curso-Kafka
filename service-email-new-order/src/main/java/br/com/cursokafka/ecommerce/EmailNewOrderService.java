package br.com.cursokafka.ecommerce;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.cursokafka.ecommerce.consumer.ConsumerService;
import br.com.cursokafka.ecommerce.consumer.ServiceRunner;
import br.com.cursokafka.ecommerce.dispatcher.KafkaDispatcher;

public class EmailNewOrderService implements ConsumerService<Order> {
	
	private final KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<>();

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String[] args)  {
		new ServiceRunner(EmailNewOrderService::new).start(1);
	}
	
	public void parse(ConsumerRecord<String, Message<Order>> record) throws InterruptedException, ExecutionException {
		System.out.println("-----------------------------------------");
		System.out.println("Processing new order, preparing email ");
		System.out.println(record.value());
		
		var emailCode = "Thank you for your order, we are processing your order!";
		var message = record.value();
		var id = message.getId().continueWith(EmailNewOrderService.class.getSimpleName());
		var order = message.getPayload();
		emailDispatcher.send("ECOMMERCE_SEND_EMAIL",
				order.getEmail(),
				emailCode,
				id);
	}

	@Override
	public String getConsumerGroup() {
		return EmailNewOrderService.class.getSimpleName();
	}

	@Override
	public String getTopic() {
		return "ECOMMERCE_NEW_ORDER";
	}
	
}
