package net.lshift.rabbitmq;

import java.io.IOException;

import com.rabbitmq.client.Connection;
import com.rabbitmq.messagepatterns.unicast.ConnectionListener;
import com.rabbitmq.messagepatterns.unicast.Thunk;

/**
 * Extension of the MP implementation to support Exchange deletion.
 */
public class SenderImpl extends com.rabbitmq.messagepatterns.unicast.impl.SenderImpl {
    
    public void deleteExchange() throws Exception {
        while (true) {
            if (connector.attempt(new Thunk() {
                public void run() throws IOException {
                    channel.exchangeDelete(getExchangeName(), false);
                    if (transactional) channel.txCommit();
                }
            }, new ConnectionListener() {
                public void connected(Connection conn) throws IOException {
                    connect(conn);
                }})) break;
        }
    }
}
