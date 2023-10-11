package br.com.cursokafka.ecommerce.consumer;

public interface ServiceFactory<T> {

	ConsumerService<T> create();
}
