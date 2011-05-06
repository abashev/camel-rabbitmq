package net.lshift.camel.component.rabbitmq;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

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
    private final Logger LOG = LoggerFactory.getLogger(RabbitMqConsumer.class);

    private String queueName;
    private String exchangeName;
    private MessageReceiver receiver;
    private ExecutorService executor;

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
    }

    @Override
    public void stop() throws Exception {
        LOG.debug("Stopping RabbitMQConsumer on queue {} bound to exchange {}...", queueName, exchangeName);

        if (executor != null) {
            shutdownAndAwaitTermination(executor);
        }

        receiver.stop();
        super.stop();
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
                LOG.debug("Received message from queue {}", queueName);

                Exchange exchange = getEndpoint().createExchange();

                Message msg = new DefaultMessage();

                msg.setBody(message.getBody());
                msg.setHeaders(message.getProperties().getHeaders());
                exchange.setIn(msg);

                getProcessor().process(exchange);

                receiver.ack(message);
            }
        }
    }

    void shutdownAndAwaitTermination(ExecutorService pool) {
        try {
            pool.shutdown();
            pool.shutdownNow();

            if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
                LOG.warn(
                        "ExecutionPool for [queue={},exchange={}] did not terminate",
                        queueName, exchangeName
                );
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            pool.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }
}
