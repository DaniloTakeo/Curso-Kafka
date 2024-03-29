package br.com.cursokafka.ecommerce.dispatcher;

import org.apache.kafka.common.serialization.Serializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import br.com.cursokafka.ecommerce.Message;
import br.com.cursokafka.ecommerce.MessageAdapter;

public class GsonSerializer<T> implements Serializer<T> {
	
	private final Gson gson = new GsonBuilder().registerTypeAdapter(Message.class, new MessageAdapter()).create();

	@Override
	public byte[] serialize(String topic, T data) {
		return gson.toJson(data).getBytes();
	}

}
