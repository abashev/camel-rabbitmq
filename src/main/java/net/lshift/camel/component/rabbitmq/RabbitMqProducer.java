package net.lshift.camel.component.rabbitmq;

import java.io.IOException;

import net.lshift.rabbitmq.MessageSender;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.messagepatterns.unicast.Message;

public class RabbitMqProducer extends DefaultProducer {

    private static transient Logger LOG = LoggerFactory.getLogger(RabbitMqProducer.class);

    private String exchangeName;

    private MessageSender sender;

	public RabbitMqProducer(RabbitMqEndpoint endpoint, String uri) throws Exception {
		super(endpoint);
		parseUri(uri);
		sender = new MessageSender(endpoint.getConnection(), exchangeName);
	}

	protected void parseUri(String uri) {
	    String[] split = uri.split("[:|/]");
	    if(split.length != 4) {
	        throw new IllegalArgumentException("Invalid URI - must contain an exchange: " + uri);
	    }

	    exchangeName = split[3];
	}

	@Override
	public void start() throws Exception {
	    LOG.info("Starting RabbitMqProducer on exchange {}...", exchangeName);
		super.start();
		sender.start();
		LOG.info("RabbitMqProducer started.");
	}

	@Override
	public void stop() throws IOException {
	    LOG.info("Stopping RabbitMqProducer...");
		if(sender != null) {
			sender.stop();
		}
		LOG.info("RabbitMqProducer stopped.");
	}

	public void process(Exchange exchange) throws Exception {
        Message message = sender.createMessage();

        message.getProperties().setHeaders(exchange.getIn().getHeaders());
        message.setBody(exchange.getIn().getBody(byte[].class));

        LOG.debug("Sending message to exchange {}", exchangeName);

        sender.send(message);
    }
}
