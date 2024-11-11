package sn_training.string;

import org.apache.pulsar.client.api.PulsarClient;
import sn_training.Config;
import sn_training.DynamicProducerFactory;
import sn_training.MessageGenerator;
import sn_training.util.AsyncRunner;
import sn_training.util.ShutdownHook;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * 
 * mvn compile exec:java@order_producer
 * 
 * This class produces to a different topic based on the content of the message.
 *
 * This is a very common pattern in messaging systems and is often used to route
 * traffic to a large number of topics. We use the DynamicProducerFactory to create producers.
 *
 * We also use this exercise to introduce the async APIs which
 * are what you should prefer to use in most production code.
 *
 * You will notice that means you need to work in terms of CompletableFuture.
 * This guide https://www.baeldung.com/java-completablefuture gives a good intro if you aren't
 * familiar with it
 *
 */
public class OrderProducer extends AsyncRunner {

    DynamicProducerFactory producerFactory;
    MessageGenerator messageGenerator;

    @Override
    protected CompletableFuture<?> workLoop() {

        MessageGenerator.StringMessage message = messageGenerator.nextStringMessage();

        String topic;
        if (message.getValue().contains("China")) {
            topic = Config.Topics.ORDER_BACKLOG_CHINA;
        } else {
            topic = Config.Topics.ORDER_BACKLOG_US;
        }

        System.out.println("Publising message: " + message + " async to topic: " + topic);

        return producerFactory.getProducer(topic).thenCompose((producer)
                -> producer.sendAsync(message.getValue()));
    }

    // The AsyncRunner calls this class to do initialization
    @Override
    protected CompletableFuture<Void> setup(PulsarClient client) {
        //override loop time here if desired, default in AsyncRunner is 1000 if none is specified in setup
        setLoopTime(1000); 

        producerFactory = new DynamicProducerFactory(client);

        messageGenerator = new MessageGenerator();

        // create an already completed future to satisfy the interface
        CompletableFuture<Void> future = new CompletableFuture<>();
        future.complete(null);
        return future;
    }

    // The entrypoint to the application
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        // Create our producer, register the shutdown hook, and then run
        OrderProducer orderProducer = new OrderProducer();
        ShutdownHook.registerForShutdownHook(orderProducer);
        //executing run will execute setup, followed by the doWork loop which calls the workLoop repeatedly
        //since the workLoop never finished, get will never complete and program will not exit
        orderProducer.run().get();
    }
}