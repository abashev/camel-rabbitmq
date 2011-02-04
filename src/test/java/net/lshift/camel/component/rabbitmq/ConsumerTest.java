package net.lshift.camel.component.rabbitmq;

import java.io.IOException;

import net.lshift.rabbitmq.MessageSender;
import net.lshift.rabbitmq.MessageTest;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.After;
import org.junit.Test;

import com.rabbitmq.messagepatterns.unicast.Message;

// TODO: Test what happens with no exchange
public class ConsumerTest extends CamelTestSupport {
    
    private String MESSAGE = "TEST_MESSAGE_CONTENT";
    
    private MessageSender sender;
    
	@Test
	public void consumerConsumes() throws Exception {
		MockEndpoint mock = getMockEndpoint("mock:result");
		mock.expectedBodiesReceived(MESSAGE);
		sendMessage(MESSAGE);
		assertMockEndpointsSatisfied();
	}
	
	@After
    public void shutdownSender() throws IOException {
        sender.stop();
    }
		
	protected RouteBuilder routeBuilder = new RouteBuilder() {
	    public void configure() {
            from("rabbitmq://TESTQ:TESTX").to("mock:result");
        }
	};

	@Override
	protected RouteBuilder createRouteBuilder() {
	    // Ensure sender is created before consumer
	    try {
            sender = new MessageSender(MessageTest.createConnector(), "TESTX");
            sender.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
	    
		return routeBuilder;
	}
	
	protected void sendMessage(String msg) throws Exception {
	    Message message = sender.createMessage();
	    message.setBody(msg.getBytes());
	    sender.send(message);
	}
}
