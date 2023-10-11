package br.com.cursokafka.ecommerce;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import br.com.cursokafka.ecommerce.consumer.KafkaService;

public class LogService {

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		var logService = new LogService();
		
		try(var kafkaService = new KafkaService<String>(LogService.class.getSimpleName(),
				Pattern.compile("ECOMMERCE.*"),
				logService::parse,
				Map.of(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()))) {
			kafkaService.run();
		}
	}
	
	public void parse(ConsumerRecord<String, Message<String>> record) {		
		System.out.println("-----------------------------------------");
		System.out.println("LOG");
		System.out.println(record.topic());
		System.out.println(record.key());
		System.out.println(record.value());
		System.out.println(record.partition());
		System.out.println(record.offset());
	}
}
