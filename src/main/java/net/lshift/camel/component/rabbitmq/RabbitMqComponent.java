package net.lshift.camel.component.rabbitmq;

import java.io.IOException;
import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.impl.DefaultComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.messagepatterns.unicast.ConnectionBuilder;
import com.rabbitmq.messagepatterns.unicast.Connector;
import com.rabbitmq.messagepatterns.unicast.Factory;


public class RabbitMqComponent extends DefaultComponent {

    private static transient Logger LOG = LoggerFactory.getLogger(RabbitMqComponent.class);
    
    private ConnectionFactory connectionFactory;
    
    public RabbitMqComponent() {
    }

    public RabbitMqComponent(CamelContext context) {
        super(context);
    }
    
    @Override
    protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
        assert LOG.isDebugEnabled();
        LOG.debug("Creating new RabbitMQ endpoint for {}", uri);
    	return new RabbitMqEndpoint(uri, this);
    }
    
    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }
    
    protected Connector getConnector() {
        return Factory.createConnector(new ConnectionBuilder() {
            public Connection createConnection() throws IOException {
                return (connectionFactory == null) ? 
                        new ConnectionFactory().newConnection() : connectionFactory.newConnection();
            }
        });
    }
}