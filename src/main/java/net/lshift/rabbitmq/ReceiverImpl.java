package net.lshift.rabbitmq;

import java.io.IOException;

import com.rabbitmq.client.Connection;
import com.rabbitmq.messagepatterns.unicast.ConnectionListener;
import com.rabbitmq.messagepatterns.unicast.Thunk;

/**
 * Extension of the MP implementation to support Queue deletion.
 */
public class ReceiverImpl extends com.rabbitmq.messagepatterns.unicast.impl.ReceiverImpl {
    
    public void deleteQueue() throws Exception {
        while (true) {
            if (connector.attempt(new Thunk() {
                public void run() throws IOException {
                    channel.queueDelete(getQueueName(), false, false);
                }
            }, new ConnectionListener() {
                public void connected(Connection conn) throws IOException {
                    connect(conn);
                }})) break;
        }
    }
}
