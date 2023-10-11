package br.com.cursokafka.ecommerce.consumer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.StringDeserializer;

import br.com.cursokafka.ecommerce.Message;
import br.com.cursokafka.ecommerce.dispatcher.GsonSerializer;
import br.com.cursokafka.ecommerce.dispatcher.KafkaDispatcher;

public class KafkaService<T> implements Closeable {

	private final String groupId;
	private KafkaConsumer<String, Message<T>> consumer;
	private String topic;
	private final ConsumerFunction<T> parse;

	public KafkaService(String groupId, String topic, ConsumerFunction<T> parse, Map<String, String> properties) {
		this(parse, groupId, properties);
		this.topic = topic;
		this.consumer.subscribe(Collections.singletonList(this.topic));
	}

	public KafkaService(String groupId, Pattern pattern, ConsumerFunction<T> parse, Map<String, String> properties) {
		this(parse, groupId, properties);
		this.consumer.subscribe(pattern);
	}

	private KafkaService(ConsumerFunction<T> parse, String groupId, Map<String, String> properties) {
		this.groupId = groupId;
		this.parse = parse;
		this.consumer = new KafkaConsumer<String, Message<T>>(getProperties(properties));
	}

	@SuppressWarnings("resource")
	public void run() throws ExecutionException, InterruptedException {
		try(var deadLetterDispatcher = new KafkaDispatcher<>()) {
			while(true) {
				var records = consumer.poll(Duration.ofMillis(100));
				
				if(!records.isEmpty()) {
					System.out.println("It was found " + records.count() + " records");
					
					for(var record : records) {
						try {
							parse.consume(record);
						} catch (Exception e) {
							e.printStackTrace();
							var message = record.value();
							deadLetterDispatcher.send("ECOMMERCE_DEADLETTER",
									message.getId().toString(),
									new GsonSerializer<>().serialize("", message),
									message.getId().continueWith("DeadLetter"));
						}
					}
				}
			}					
		}
	}
	
	private Properties getProperties(Map<String, String> overrideProperties) {
		var properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.16:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
		properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, Uuid.randomUuid().toString());
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
		//properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		properties.putAll(overrideProperties);
		
		return properties;
	}

	@Override
	public void close() {
		this.consumer.close();
	}

}
