package br.com.cursokafka.ecommerce;

import java.math.BigDecimal;

public class Order {
	
	private final String orderId;
	private final BigDecimal amount;
	private final String email;
	
	public Order(String orderId, BigDecimal amount, String email) {
		this.orderId = orderId;
		this.amount = amount;
		this.email = email;
	}


	public String getOrderId() {
		return orderId;
	}

	public BigDecimal getAmount() {
		return amount;
	}
	
	public String getEmail() {
		return email;
	}

	@Override
	public String toString() {
		return "Order { " +
					"\n\torderId: " + this.orderId +
					"\n\tamount: " + this.amount.toString() +
					"\n\temail: " + this.email +
					"}";
	}
}
