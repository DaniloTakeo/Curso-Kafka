package br.com.cursokafka.ecommerce.consumer;

import org.apache.kafka.common.serialization.Deserializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import br.com.cursokafka.ecommerce.Message;
import br.com.cursokafka.ecommerce.MessageAdapter;

public class GsonDeserializer implements Deserializer<Message<?>> {

	private final Gson gson = new GsonBuilder().registerTypeAdapter(Message.class, new MessageAdapter()).create();

	@Override
	public Message<?> deserialize(String topic, byte[] data) {
		return gson.fromJson(new String(data), Message.class);
	}
}
