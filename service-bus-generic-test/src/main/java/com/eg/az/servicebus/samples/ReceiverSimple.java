package com.eg.az.servicebus.samples;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.azure.core.amqp.AmqpRetryOptions;
import com.azure.core.amqp.AmqpTransportType;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusProcessorClient;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;

public class ReceiverSimple {

	private final static Logger log = Logger.getLogger("com.eg.az.servicebus.samples");

	private String connectionString = System.getenv("CONNECTION_STRING");
	private String queueName = System.getenv("QUEUE_NAME");

	public static void main(String args[]) {
		new ReceiverSimple().receiveMessages0();
	}

	// handles received messages
	void receiveMessages0() {

		ServiceBusClientBuilder builder = new ServiceBusClientBuilder();
		AmqpRetryOptions amqpOptions = new AmqpRetryOptions();

		amqpOptions.setTryTimeout(Duration.ofSeconds(30)); // default 60 sec
		amqpOptions.setMaxDelay(Duration.ofSeconds(60)); // default 60 sec
		//amqpOptions.setDelay(Duration.ofMillis(800)); // default 0.8 sec
		//amqpOptions.setMaxRetries(5);

		builder.transportType(AmqpTransportType.AMQP);
		builder.retryOptions(amqpOptions);

		ServiceBusProcessorClient processorClient = builder.connectionString(connectionString).processor()
				.queueName(queueName).processMessage(context -> {
					ServiceBusReceivedMessage message = context.getMessage();
					log.info("Processing message. Session: " + message.getMessageId() + ", Sequence #: "
							+ message.getSequenceNumber());

				}).processError(context -> {
					context.getException().printStackTrace();
				}).buildProcessorClient();

		log.info("Starting,..");
		processorClient.start();

		try {
			TimeUnit.SECONDS.sleep(10);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Stopping and closing the processor");
		processorClient.close();

	}

}
