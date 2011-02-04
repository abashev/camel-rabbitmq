package net.lshift.rabbitmq;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.messagepatterns.unicast.ConnectionBuilder;
import com.rabbitmq.messagepatterns.unicast.Connector;
import com.rabbitmq.messagepatterns.unicast.Factory;
import com.rabbitmq.messagepatterns.unicast.Message;
import com.rabbitmq.messagepatterns.unicast.ReceivedMessage;

public class MessageTest {
    
    protected static final String QUEUE_NAME = "TEST_QUEUE";
    
    protected static final String EXCHANGE_NAME = "TEST_EXCHANGE";
    
    protected static final String MESSAGE_BODY = "TEST_MESSAGE_BODY";

    protected MessageSender sender;

    protected MessageReceiver receiver;

    
    public static Connector createConnector() {
        return Factory.createConnector(new ConnectionBuilder() {
            public Connection createConnection() throws IOException {
                return new ConnectionFactory().newConnection();
            }
        });
    }
    
    @Before
    public void initMessaging() throws Exception {
        sender = new MessageSender(createConnector(), EXCHANGE_NAME);
        sender.setTransactional(true);
        sender.start();
        
        receiver = new MessageReceiver(createConnector(), QUEUE_NAME, EXCHANGE_NAME);
        receiver.start();
    }
    
    @Test
    public void senderSendsMessagesAndReceiverReceivesThem() throws Exception {
        Message msg = sender.createMessage();
        msg.setBody(MESSAGE_BODY.getBytes());
        sender.send(msg);
        
        ReceivedMessage msg2 = receiver.receive(1000);
        receiver.ack(msg2);
        assertEquals(MESSAGE_BODY, new String(msg2.getBody()));
        
        sender.stop();
        receiver.stop();
    }
    
    @Test
    public void receiverCanBeStoppedAndStarted() throws Exception {
        receiver.stop();
        receiver.start();
        
        senderSendsMessagesAndReceiverReceivesThem();
    }
    
    @Test
    public void senderCanBeStoppedAndStarted() throws Exception {
        Message msg = sender.createMessage();
        msg.setBody("HELLO".getBytes());
        
        sender.stop();
        
        try {
            sender.send(msg);
            fail("Expected exception sending to closed connection");
        } catch(AlreadyClosedException e) {
            // success
        }
        
        sender.start();        
        sender.send(msg);
    }
}
