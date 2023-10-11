package br.com.cursokafka.ecommerce;

public class Email {

	private final String subject;
	private final String body;
	
	public Email(String subject, String body) {
		this.subject = subject;
		this.body = body;
	}

	public String getSubject() {
		return subject;
	}

	public String getBody() {
		return body;
	}
	
	
}
