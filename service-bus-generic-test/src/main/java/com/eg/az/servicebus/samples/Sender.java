package com.eg.az.servicebus.samples;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.azure.core.amqp.AmqpRetryOptions;
import com.azure.core.amqp.AmqpTransportType;
import com.azure.core.util.ClientOptions;
import com.azure.core.util.Configuration;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusSenderClient;

public class Sender {

	private final static Logger log = Logger.getLogger("com.eg.az.servicebus.samples");

	private String connectionString = System.getenv("CONNECTION_STRING"); 
	private String queueName = System.getenv("QUEUE_NAME"); 

	private int messagesPerClient = 100;
	private int dataLen = 100;
	private long intervPerSend = 3000;

	public static void main(String args[]) {
		new Sender().perform();
	}

	private void perform() {
		// TODO Auto-generated method stub
		new Sender().send();
	}

	private void send() {
		// TODO Auto-generated method stub
		
		ServiceBusClientBuilder builder = new ServiceBusClientBuilder();
		AmqpRetryOptions amqpOptions = new AmqpRetryOptions();
		ClientOptions cliOptions = new ClientOptions();
		Configuration config = new Configuration();
		
		builder.transportType(AmqpTransportType.AMQP);
		builder.configuration(config);
		builder.clientOptions(cliOptions);
		builder.retryOptions(amqpOptions);

		ServiceBusSenderClient senderClient = builder
				.connectionString(connectionString).sender()
				.queueName(queueName).buildClient();

		int sentCounter = 0;

		while(true) {
			// send one message to the queue
			for (int i = 0; i < messagesPerClient; i++) {
				TelemetryDataPoint tdp = new TelemetryDataPoint(true, dataLen);
				String msgStr = tdp.serialize();

				senderClient.sendMessage(new ServiceBusMessage(msgStr));
				log.info(Thread.currentThread().getName() + " " + ++sentCounter);

				try {
					Thread.currentThread().sleep(intervPerSend);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			try {
				Thread.currentThread().sleep(3000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		//senderClient.close();
	}

}
