package br.com.cursokafka.ecommerce;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.cursokafka.ecommerce.consumer.ConsumerService;
import br.com.cursokafka.ecommerce.consumer.ServiceRunner;

public class EmailService implements ConsumerService<String> {
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String[] args) throws ExecutionException, InterruptedException {
		new ServiceRunner(EmailService::new).start(5);
	}
	
	public String getConsumerGroup() {
		return EmailService.class.getSimpleName();
	}
	
	public String getTopic() {
		return "ECOMMERCE_SEND_EMAIL";
	}
	
	public void parse(ConsumerRecord<String, Message<String>> record) {
		var message = record.value();
		
		System.out.println("-----------------------------------------");
		System.out.println("Sending email");
		System.out.println(record.key());
		System.out.println(message.getPayload());
		System.out.println(record.partition());
		System.out.println(record.offset());
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		System.out.println("The email has been sent");
	}

}
