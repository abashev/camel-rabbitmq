package net.lshift.camel.component.rabbitmq;

import net.lshift.rabbitmq.MessageReceiver;
import net.lshift.rabbitmq.MessageTest;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.rabbitmq.messagepatterns.unicast.ReceivedMessage;

public class ProducerTest extends CamelTestSupport {

    private String MESSAGE = "TEST_MESSAGE_CONTENT";
    
    private MessageReceiver receiver;
    
    
    @Before
    public void setupReceiver() throws Exception {
        receiver = new MessageReceiver(MessageTest.createConnector(), "TESTQ", "TESTX");
        receiver.start();
    }
    
    @After
    public void shutdownReceiver() throws Exception {
        receiver.stop();
    }
    
	@Test
	public void testSendStringContent() throws Exception {
	    template.sendBody("direct:in", MESSAGE);	
	    assertMessageReceived(MESSAGE);
	}

	@Test
	public void testSendBinaryContent() throws Exception {
	    template.sendBody("direct:in", MESSAGE.getBytes());
	    assertMessageReceived(MESSAGE);
	}
	
	protected RouteBuilder createRouteBuilder() {
		return new RouteBuilder() {
			public void configure() {
				from("direct:in").to("rabbitmq://TESTX");
			}
		};
	}
	
	protected void assertMessageReceived(String msg) throws Exception {
	    ReceivedMessage message = receiver.receive(1000);
	    assertEquals(msg, new String(message.getBody()));
	}
}
