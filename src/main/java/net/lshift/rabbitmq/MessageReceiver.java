package net.lshift.rabbitmq;

import static net.lshift.rabbitmq.DefaultChannelSetupHandler.DURABLE;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.messagepatterns.unicast.ChannelSetupListener;
import com.rabbitmq.messagepatterns.unicast.Connector;
import com.rabbitmq.messagepatterns.unicast.ReceivedMessage;

/**
 * Thin wrapper around the Java Message Patterns receiver implementation so that
 * we benefit from the channel / connection reliability, whilst hiding the
 * Unicast abstractions that are not needed here.
 */
public class MessageReceiver {
    private final Logger LOG = LoggerFactory.getLogger(MessageReceiver.class);

    protected ReceiverImpl receiver = new ReceiverImpl();

    public MessageReceiver(Connector connector, String queueName, String exchangeName) throws Exception {
        receiver.setConnector(connector);
        receiver.setQueueName(queueName);

        addChannelSetupListener(new DefaultChannelSetupHandler(exchangeName, queueName));
    }

    public Connector getConnector() {
        return receiver.getConnector();
    }

    public String getQueueName() {
        return receiver.getQueueName();
    }

    public void setQueueName(String queueName) {
        receiver.setQueueName(queueName);
    }

    public void addChannelSetupListener(ChannelSetupListener channelSetup) {
        receiver.addSetupListener(channelSetup);
    }

    public void removeChannelSetupListener(ChannelSetupListener channelSetup) {
        receiver.removeSetupListener(channelSetup);
    }

    public void addChannelReceiverChannelSetupListener(ChannelSetupListener channelSetup) {
        receiver.addSetupListener(channelSetup);
    }

    public void removeReceiverChannelSetupListener(ChannelSetupListener channelSetup) {
        receiver.removeSetupListener(channelSetup);
    }

    public void start() throws Exception {
        LOG.debug("Starting MessageReceiver on queue {}...", getQueueName());

        receiver.init();
    }

    public void stop() throws IOException {
        if (!DURABLE) {
            try {
                receiver.deleteQueue();
            } catch(Exception e) {
                LOG.warn("Unable to delete queue " + getQueueName(), e);
            }
        }

        try {
            receiver.close();
        } catch (AlreadyClosedException e) {
        } catch (ShutdownSignalException e) {
            if (!e.isInitiatedByApplication()) {
                throw e;
            }
        } catch (Exception e) {
            if (e instanceof IOException) {
                throw (IOException) e;
            } else {
                throw new RuntimeException(e);
            }
        }

        LOG.debug("Stopped MessageReceiver on queue {}...", getQueueName());
    }

    public ReceivedMessage receive() throws Exception {
        try {
            return receiver.receive();
        } catch(InterruptedException e) {
            // Interrupt on blocking queue - ignore and return null
            LOG.warn("Interrupt on blocking queue");
            return null;
        }
    }

    public ReceivedMessage receive(long timeout) throws Exception {
        return receiver.receive(timeout);
    }

    public ReceivedMessage receiveNoWait() throws Exception {
        return receiver.receiveNoWait();
    }

    public void ack(ReceivedMessage m) throws Exception {
        receiver.ack(m);
    }

    public void cancel() throws IOException {
        receiver.cancel();
    }
}