package br.com.cursokafka.ecommerce.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.cursokafka.ecommerce.Message;

public interface ConsumerService<T> {

	String getConsumerGroup();
	String getTopic();
	void parse(ConsumerRecord<String, Message<T>> record) throws Exception;
}
