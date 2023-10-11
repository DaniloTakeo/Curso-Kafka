package br.com.cursokafka.ecommerce;

import java.math.BigDecimal;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.common.Uuid;

import br.com.cursokafka.ecommerce.dispatcher.KafkaDispatcher;

public class NewOrderMain {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		try (var orderDispatcher = new KafkaDispatcher<Order>()) {
			var orderId = Uuid.randomUuid().toString();
			var emailAddress = Math.random() + "@xpto.com";
			for (int i = 0; i < 10; i++) {
				var amount = new BigDecimal(Math.random() * 5000 + 1);

				var id = new CorrelationId(NewOrderMain.class.getSimpleName());
				var order = new Order(orderId, amount, emailAddress);
				orderDispatcher.send("ECOMMERCE_NEW_ORDER", emailAddress, order, id);
			}
		}
	}

}
