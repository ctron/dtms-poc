package de.dentrassi.kapua.dtms.poc1;

import java.util.Enumeration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;

import org.apache.qpid.jms.message.JmsMessage;

public class ServiceAImpl implements ServiceA {

	private ConnectionFactory factory;

	private MessageProducer internalProducer;

	private Destination internalQueue;
	private Destination externalTopic;

	private Session requestSession;

	private MessageProducer externalProducer;

	private Session internalSession;

	private Destination dlqDestination;

	public ServiceAImpl() throws Exception {
		Context context = new InitialContext();

		this.factory = (ConnectionFactory) context.lookup("broker");
		this.internalQueue = (Destination) context.lookup("serviceAInternal");
		this.externalTopic = (Destination) context.lookup("serviceAExternal");
		this.dlqDestination = (Destination) context.lookup("DLQ");

		// set up client service connection
		{
			Connection connection = factory.createConnection(System.getProperty("USER"),
					System.getProperty("PASSWORD"));
			connection.start();
			this.requestSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			this.internalProducer = requestSession.createProducer(internalQueue);
		}

		// set up internal service receiver
		{
			final Connection connection = factory.createConnection(System.getProperty("USER"),
					System.getProperty("PASSWORD"));
			connection.start();

			this.internalSession = connection.createSession(true, Session.SESSION_TRANSACTED);
			final MessageConsumer consumer = internalSession.createConsumer(internalQueue);
			this.externalProducer = internalSession.createProducer(externalTopic);

			consumer.setMessageListener(this::internalDoStuff);
		}
	}

	private static final class ReplyListener implements AutoCloseable {

		private CountDownLatch received = new CountDownLatch(1);
		private MessageConsumer consumer;
		private MessageConsumer dlqConsumer;
		private String id;

		public ReplyListener(final String id, Session session, Destination destination, Destination dlqDestination)
				throws JMSException {

			this.id = id;

			// final String filter = "JMSCorrelationID='" + id + "'";
			final String filter = null;
			System.out.println("Filter: " + filter);

			this.consumer = session.createConsumer(destination, filter);
			this.consumer.setMessageListener(this::gotReply);

			this.dlqConsumer = session.createConsumer(dlqDestination, filter);
			this.dlqConsumer.setMessageListener(this::gotReply);
		}

		private void gotReply(Message message) {
			try {

				final Enumeration<?> en = ((JmsMessage) message).getAllPropertyNames();
				while (en.hasMoreElements()) {
					final String key = (String) en.nextElement();
					System.out.format("%s => %s%n", key, message.getObjectProperty(key));
				}

				if (id.equals(message.getJMSCorrelationID())) {
					System.out.println("Got result");
					received.countDown();
				}
			} catch (JMSException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void close() throws Exception {
			consumer.close();
			dlqConsumer.close();
		}

		public void await() throws InterruptedException {
			received.await();
		}

	}

	/**
	 * External API for doStuff
	 */
	@Override
	public void doStuff(boolean requestFailure) {
		try {
			String id = UUID.randomUUID().toString();
			MapMessage message = requestSession.createMapMessage();

			message.setBoolean("requestFailure", requestFailure);
			message.setJMSCorrelationID(id);

			// set up reply listener

			try (ReplyListener reply = new ReplyListener(id, requestSession, externalTopic, dlqDestination)) {

				// send internal request

				internalProducer.send(message);
				reply.await();
			}

		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Internal method for doStuff
	 */
	protected void internalDoStuff(final Message message) {

		try {

			System.out.format("Received - ID: %s -> %s%n", message.getJMSCorrelationID(), message);

			if (!(message instanceof MapMessage)) {
				// non-recoverable, message gets consumed
				internalSession.commit();
				return;
			}

			// extract input parameters

			boolean value = ((MapMessage) message).getBoolean("requestFailure");

			// process business logic

			String result = businessLogic(value);

			// pack up result

			TextMessage resultMessage = internalSession.createTextMessage(result);
			resultMessage.setJMSCorrelationID(message.getJMSCorrelationID());

			// send

			externalProducer.send(resultMessage);
			internalSession.commit();

		} catch (Exception e) {
			try {
				internalSession.rollback();
			} catch (JMSException e1) {
				throw new RuntimeException(e1);
			}
		}

	}

	private String businessLogic(boolean crashAndBurn) {
		System.out.println("Produce failure? -> " + crashAndBurn);

		if (crashAndBurn) {
			throw new RuntimeException("Business logic error");
		}

		return "External: " + crashAndBurn;
	}

}
