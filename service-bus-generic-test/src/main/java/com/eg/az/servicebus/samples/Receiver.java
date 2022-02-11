package com.eg.az.servicebus.samples;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.azure.core.amqp.AmqpRetryOptions;
import com.azure.core.amqp.AmqpTransportType;
import com.azure.core.util.IterableStream;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;


public class Receiver {

	private final static Logger log = Logger.getLogger("com.eg.az.servicebus.samples");
	
	private String connectionString = System.getenv("CONNECTION_STRING");
	private String queueName = System.getenv("QUEUE_NAME");

	public static void main(String args[]) {
		new Receiver().receiveMessages2();
	}
	
	int counter = 0;

	void receiveMessages2(){
		
		ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
		ScheduledFuture<?> future = executor.scheduleWithFixedDelay(() -> {
			log.info("(!) Recieving data"+System.currentTimeMillis()+" ("+(counter++)+")");
			receiveMessages1();
		}, 1, 2*60, TimeUnit.SECONDS);	//	repeating to run receiveMessages1() with delays  
		
		try {
			Thread.sleep(60 * 1000);	//	wait for 60 seconds
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		future.cancel(true);
		
	}

	void receiveMessages1(){
		
		ServiceBusClientBuilder builder = new ServiceBusClientBuilder();
		AmqpRetryOptions amqpOptions = new AmqpRetryOptions();

		amqpOptions.setTryTimeout(Duration.ofSeconds(30)); // default 60 sec
		amqpOptions.setMaxDelay(Duration.ofSeconds(60)); // default 60 sec
		//amqpOptions.setDelay(Duration.ofMillis(800)); // default 0.8 sec
		//amqpOptions.setMaxRetries(5);

		builder.transportType(AmqpTransportType.AMQP);
		builder.retryOptions(amqpOptions);
		
		ServiceBusReceiverClient client = builder.connectionString(connectionString)
				.receiver().queueName(queueName).buildClient();
		
		//	fetching simply one by one
		//
		//ServiceBusReceivedMessage message = null;
		//while ((message = client.peekMessage()) != null) {
		//	System.out.println(message.getApplicationProperties());
		//	log.info("Processing message. Session: "+message.getMessageId()+", Sequence #: "+message.getSequenceNumber());
		//}
		
		//	fetching in batch mode
		//
		IterableStream<ServiceBusReceivedMessage> msgs = client.receiveMessages(100, Duration.ofMinutes(3));
		while((msgs = client.receiveMessages(30, Duration.ofMinutes(1))) != null) {
			msgs.forEach(message -> {
				try {
					doSomething(message);		//	your business logic here
					client.complete(message);	//	complete your message without tx
				}
				catch(Throwable th) {
					log.log(Level.SEVERE, "Failed in doSomething", message);
					client.defer(message);		//	dead lettering
				}
			});
		}
		
		client.close();
        System.out.println("done");

	}

	private void doSomething(ServiceBusReceivedMessage message) {
		// TODO Auto-generated method stub
		log.info("Processing message. Session: "+message.getMessageId()+", Sequence #: "+message.getSequenceNumber());
	}
	
}
