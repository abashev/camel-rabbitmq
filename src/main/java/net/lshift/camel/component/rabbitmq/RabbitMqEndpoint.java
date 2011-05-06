package net.lshift.camel.component.rabbitmq;

import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.messagepatterns.unicast.Connector;

public class RabbitMqEndpoint extends DefaultEndpoint {

    private static transient Logger LOG = LoggerFactory.getLogger(RabbitMqEndpoint.class);

	private Connector conn;

	public RabbitMqEndpoint(String uri, RabbitMqComponent component) {
		super(uri, component);
	}

	public RabbitMqEndpoint(String uri) {
        super(uri);
    }

	@Override
	public void start() throws Exception {
	    LOG.info("Starting RabbitMQ Endpoint for endpoint {}...", getEndpointUri());

	    super.start();
        conn = ((RabbitMqComponent) getComponent()).getConnector();
	}

	public void stop() throws Exception {
		if (conn != null) {
			conn.close();
		}

		super.stop();

        LOG.info("RabbitMQ Endpoint for {} was stopped.", getEndpointUri());
	}

	public boolean isSingleton() {
		return true;
	}

	public Consumer createConsumer(Processor processor) throws Exception {
		return new RabbitMqConsumer(this, processor, getEndpointUri());
	}

	public Producer createProducer() throws Exception {
		return new RabbitMqProducer(this, getEndpointUri());
	}

	public Connector getConnection() {
		return conn;
	}
}
