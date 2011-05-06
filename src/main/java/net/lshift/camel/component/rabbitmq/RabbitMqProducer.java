package net.lshift.camel.component.rabbitmq;

import java.io.IOException;

import net.lshift.rabbitmq.MessageSender;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.messagepatterns.unicast.Message;

public class RabbitMqProducer extends DefaultProducer {
    private final Logger LOG = LoggerFactory.getLogger(RabbitMqProducer.class);

    private String exchangeName;
    private final MessageSender sender;

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
	}

	@Override
	public void stop() throws IOException {
	    LOG.info("Stopping RabbitMRqProducer on exchange {}...", exchangeName);

	    try {
            sender.stop();
        } catch (Exception e) {
            log.warn("Unable to stop RabbitMRqProducer on exchange " + exchangeName, e);
        }

	    try {
            super.stop();
        } catch (Exception e) {
            if (e instanceof IOException) {
                throw (IOException) e;
            } else {
                throw new IOException(e);
            }
        }
	}

	public void process(Exchange exchange) throws Exception {
        Message message = sender.createMessage();

        message.getProperties().setHeaders(exchange.getIn().getHeaders());
        message.setBody(exchange.getIn().getBody(byte[].class));

        LOG.debug("Sending message to exchange {}", exchangeName);

        sender.send(message);
    }
}
