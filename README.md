# camel-rabbitmq

## Introduction

This is an experimental [Apache Camel](http://camel.apache.org/ "Apache Camel") Component for interacting with [RabbitMQ](http://www.rabbitmq.com/ "RabbitMQ").

The AMQP component that comes with Camel is based on the Qpid 0.5.0 client which [does not work too well with Rabbit](http://www.rabbitmq.com/interoperability.html "Rabbit Interoperability"), so I thought it a good opportunity to experiment with custom Camel components.

Currently, the Component is limited to fit with a proof of concept I'm putting together. A Producer will create a fanout exchange and publish messages to it. A Consumer will create a queue, and bind it to a named exchange. See the Limitations section below.

## Usage

The AMQP access is a crude, crow-barred wrapper around the [Java Message Patterns library](http://hg.rabbitmq.com/rabbitmq-java-messagepatterns/) so you'll need to make sure this POM is installed. I only really wanted the re-connection support so will remove this dependency at some point.


Producer URIs have the structure: `rabbitmq:<exchange-name>`

Consumer URIs have the structure: `rabbitmq:<queue-name>:<exchange-to-bind-to>`

Spring example:

    <bean id="rabbitmq" class="net.lshift.camel.component.rabbitmq.RabbitMqComponent">
      <property name="connectionFactory">
        <bean class="com.rabbitmq.client.ConnectionFactory">
          <property name="host" value="127.0.0.1"/>
          <property name="username" value="lee"/>
          <property name="password" value="letmein"/>
        </bean>
      </property>
    </bean>

    <camel:camelContext>
        <camel:package>net.lshift.camel.experimental</camel:package>

        <camel:route id="rabbitmq-ingester1">
          <camel:from uri="direct:foo" />
          <camel:to uri="rabbitmq:ingest1" />
        </camel:route>
       
        <camel:route id="rabbitmq-consumer1">
          <camel:from uri="rabbitmq:consume1:ingest1" />
          <camel:to uri="stream:out" />
        </camel:route>

        <camel:route id="rabbitmq-consumer2">
          <camel:from uri="rabbitmq:consume2:ingest1" />
          <camel:to uri="stream:out" />
        </camel:route>
    </camel:camelContext>

## Limitations

There are many limitations and caveats at the moment:

 - Currently a connection per endpoint which won't scale past our initial assessment. At some point I'll switch to channel per endpoint and maybe give the option to group connections.

 - Consumers and Producers should do reasonable set-up and tear-down, i.e., creating a producer will create the appropriate exchange, and delete it again when the producer is stopped. However, the relevant Producer must be started before the Consumer to ensure the exchange is there for binding. Going forward I will probably add something that can do deferred binding.

 - Exchanges are all fanout, exchanges and queues are not durable. I will probably make these endpoint parameters at some point.

 - Each Consumer runs in a SingleThreadExecutor. This may be fine by need to give it some more thought.

 - I haven't even started thinking about headers, transactions and QoS!
