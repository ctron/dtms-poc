package de.dentrassi.kapua.dtms.poc1;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;

public class ExternalListener {
	public ExternalListener() throws Exception {

		Context context = new InitialContext();

		ConnectionFactory factory = (ConnectionFactory) context.lookup("broker");
		Destination externalQueue = (Destination) context.lookup("serviceAExternal");

		Connection connection = factory.createConnection(System.getProperty("USER"), System.getProperty("PASSWORD"));
		connection.start();

		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		MessageConsumer consumer = session.createConsumer(externalQueue);
		consumer.setMessageListener(this::dumpMessage);
	}

	public void dumpMessage(final Message message) {
		try {
			System.out.format("External - ID: %s - %s%n", message.getJMSCorrelationID(), message);
			if (message instanceof TextMessage) {
				System.out.println("\t" + ((TextMessage) message).getText());
			}
		} catch (Exception e) {
		}
	}
}
