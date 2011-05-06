package net.lshift.rabbitmq;

import java.io.IOException;
import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.messagepatterns.unicast.ChannelSetupListener;

/**
 * Default actions for intializing exchanges and queues.
 *
 * @author <A href="mailto:abashev at gmail dot com">Alexey Abashev</A>
 * @version $Id$
 */
class DefaultChannelSetupHandler implements ChannelSetupListener {
    private static final String EXCHANGE_TYPE = "fanout";
    private static final boolean AUTO_DELETE = false;
    private static final boolean DURABLE = false;
    private static final boolean EXCLUSIVE = true;

    private final String exchange;
    private final String queue;
    private final Logger log = LoggerFactory.getLogger(DefaultChannelSetupHandler.class);

    /**
     * @param exchange
     * @param queue
     */
    public DefaultChannelSetupHandler(String exchange, String queue) {
        this.exchange = exchange;
        this.queue = queue;
    }

    /* (non-Javadoc)
     * @see com.rabbitmq.messagepatterns.unicast.ChannelSetupListener#channelSetup(com.rabbitmq.client.Channel)
     */
    @Override
    public void channelSetup(Channel channel) throws IOException {
        if (exchange != null) {
            log.info(
                    "Declare new exchange [name={},type={},durable={},auto_delete={}]",
                    new Object[] { exchange, EXCHANGE_TYPE, DURABLE, AUTO_DELETE }
            );

            channel.exchangeDeclare(
                    exchange, EXCHANGE_TYPE, DURABLE, AUTO_DELETE, Collections.<String, Object>emptyMap()
            );
        }

        if (queue != null) {
            log.info(
                    "Declare new queue [name={},durable={},exclusive={},auto_delete={}]",
                    new Object[] { queue, DURABLE, EXCLUSIVE, AUTO_DELETE }
            );

            channel.queueDeclare(
                    queue, DURABLE, EXCLUSIVE, AUTO_DELETE, Collections.<String, Object>emptyMap()
            );
        }

        if ((queue != null) && (exchange != null)) {
            log.debug("Bind queue [{}] with exchange [{}]", queue, exchange);

            channel.queueBind(queue, exchange, "");
        }
    }
}
