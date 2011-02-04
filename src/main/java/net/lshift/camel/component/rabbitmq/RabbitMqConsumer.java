package net.lshift.camel.component.rabbitmq;

import net.lshift.rabbitmq.MessageReceiver;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultConsumer;
import org.apache.camel.impl.DefaultMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.messagepatterns.unicast.ReceivedMessage;

public class RabbitMqConsumer extends DefaultConsumer implements Runnable {
    
    private static transient Logger LOG = LoggerFactory.getLogger(RabbitMqConsumer.class);

    private String queueName;
    
    private String exchangeName;
    
    private MessageReceiver receiver;

    private java.util.concurrent.ExecutorService executor;

    public RabbitMqConsumer(RabbitMqEndpoint endpoint, Processor processor,
            String uri) throws Exception {
        super(endpoint, processor);
        parseUri(uri);
        receiver = new MessageReceiver(endpoint.getConnection(), queueName, exchangeName);        
    }
    
    public void parseUri(String uri) {
        String[] split = uri.split("[:|/]");
        if(split.length != 5) {
            throw new IllegalArgumentException("Invalid URI - must contain a queue and an exchange: " + uri);
        }
        this.queueName = split[3];
        this.exchangeName = split[4];
    }

    @Override
    public void start() throws Exception {
        LOG.debug("Starting RabbitMQConsumer on queue {} bound to exchange {}...", queueName, exchangeName);
        
        super.start();
        receiver.start();

        // TODO: Is this the right strategy?
        executor = getEndpoint().getCamelContext().getExecutorServiceStrategy()
                .newSingleThreadExecutor(this, getEndpoint().getEndpointUri());
        executor.execute(this);
        
        LOG.debug("RabbitMQConsumer stopped.");
    }

    @Override
    public void stop() throws Exception {
        LOG.debug("Stopping RabbitMQConsumer...");
        
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }

        super.doStop();
        
        LOG.debug("RabbitMQConsumer stopped.");
    }

    public void run() {
        try {
            readFromQueue();
        } catch (Exception e) {
            getExceptionHandler().handleException(e);
        }
    }

    private void readFromQueue() throws Exception {
        while (executor != null && !executor.isShutdown()) {
            ReceivedMessage message = receiver.receive();
            if(message != null) {
                LOG.debug("Recieved message from queue {}", queueName);

                Exchange exchange = getEndpoint().createExchange();

                Message msg = new DefaultMessage();
                msg.setBody(message.getBody());
                exchange.setIn(msg);
                getProcessor().process(exchange);
                receiver.ack(message);
            }
        }
    }
}
