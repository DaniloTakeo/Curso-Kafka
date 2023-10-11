package br.com.cursokafka.ecommerce;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import br.com.cursokafka.ecommerce.consumer.ConsumerService;
import br.com.cursokafka.ecommerce.consumer.ServiceRunner;


public class ReadingReportService implements ConsumerService<User> {
	
	private static final Path SOURCE = new File("src/main/resources/report.txt").toPath();

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void main(String[] args) throws ExecutionException, InterruptedException {
		new ServiceRunner(ReadingReportService::new).start(5);
	}
	
	public void parse(ConsumerRecord<String, Message<User>> record) throws IOException {
		var message = record.value();
		System.out.println("-----------------------------------------");
		System.out.println("Processing report for " + record.value());
		
		var user = message.getPayload();
		var target = new File(user.getReportPath());
		IO.copyTo(SOURCE, target);
		IO.append(target, "Created for " + user.getUuid());
		
		System.out.println("File created in: " + target.getAbsolutePath());
	}

	@Override
	public String getConsumerGroup() {
		return ReadingReportService.class.getSimpleName();
	}

	@Override
	public String getTopic() {
		return "ECOMMERCE_USER_GENERATE_READING_REPORT";
	}
}
