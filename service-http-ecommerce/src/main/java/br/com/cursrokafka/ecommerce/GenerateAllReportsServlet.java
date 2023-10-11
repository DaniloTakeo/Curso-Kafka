package br.com.cursrokafka.ecommerce;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import br.com.cursokafka.ecommerce.CorrelationId;
import br.com.cursokafka.ecommerce.dispatcher.KafkaDispatcher;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class GenerateAllReportsServlet extends HttpServlet {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final KafkaDispatcher<String> batchDispatcher = new KafkaDispatcher<>();
	
	@Override
	public void destroy() {
		super.destroy();
		batchDispatcher.close();
	}
	
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {		
		try {
			batchDispatcher.send("ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS", 
					"ECOMMERCE_USER_GENERATE_READING_REPORT", 
					"ECOMMERCE_USER_GENERATE_READING_REPORT",
					new CorrelationId(GenerateAllReportsServlet.class.getSimpleName()));
			System.out.println("Sent message report to all users");
			
			resp.setStatus(HttpServletResponse.SC_OK);
			resp.getWriter().println("Report requests generarted");
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		
	}
}
