package br.com.cursokafka.ecommerce.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.cursokafka.ecommerce.Message;

@FunctionalInterface
public interface ConsumerFunction<T> {
	
	void consume(ConsumerRecord<String, Message<T>> record) throws Exception;

}
