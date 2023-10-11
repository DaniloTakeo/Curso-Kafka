package br.com.cursrokafka.ecommerce;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

import br.com.cursokafka.ecommerce.CorrelationId;
import br.com.cursokafka.ecommerce.dispatcher.KafkaDispatcher;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class NewOrderServlet extends HttpServlet {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<Order>();
	private final KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<String>();

	
	@Override
	public void destroy() {
		super.destroy();
		orderDispatcher.close();
		emailDispatcher.close();
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		try {
			var orderId = req.getParameter("uuid");
			var emailAddress = req.getParameter("email");
			var amount = new BigDecimal(req.getParameter("amount"));
			var order = new Order(orderId, amount, emailAddress);
			
			try (var database = new OrdersDatabase()) {
				if(database.saveNew(order)) {
					orderDispatcher.send("ECOMMERCE_NEW_ORDER", emailAddress, order, new CorrelationId(NewOrderServlet.class.getSimpleName()));
					
					var email = "Welcome! we are processing your order.";
					emailDispatcher.send("ECOMMERCE_SEND_EMAIL", emailAddress, email, new CorrelationId(NewOrderServlet.class.getSimpleName()));
					
					System.out.println("New order has been sent successfully");
					resp.getWriter().println("New order has been sent");
					resp.setStatus(HttpServletResponse.SC_OK);
				} else {
					System.out.println("Old order recieved");
					resp.getWriter().println("Old order recieved");
					resp.setStatus(HttpServletResponse.SC_OK);
				}						
			}
			
		} catch (ExecutionException | InterruptedException | SQLException e) {
			throw new ServletException(e);
		}
	}

}
