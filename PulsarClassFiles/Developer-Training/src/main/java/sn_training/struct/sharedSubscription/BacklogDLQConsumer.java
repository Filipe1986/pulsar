package sn_training.struct.sharedSubscription;

import org.apache.pulsar.client.api.*;
import sn_training.Config;
import sn_training.Order;
import sn_training.util.AsyncRunner;
import sn_training.util.ShutdownHook;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * mvn compile exec:java@confirmationEmail
 * 
 * Implementing our first async consumer
 *
 **/
public class BacklogDLQConsumer extends AsyncRunner {

    CompletableFuture<Consumer<Order>> consumer;
    final String consumerTopicName = Config.StructDLQTopics.ORDER_BACKLOG_CHINA;
    final String subscriptionName = "DLQ_consumer";

    /**
     * The work loop will receive and process a message one at a time.
     *
     */
    @Override
    protected CompletableFuture<?> workLoop() {
        return consumer.thenCompose((consumer)-> handleMessage(consumer.receiveAsync())
                .thenCompose(consumer::acknowledgeAsync));
    };


    CompletableFuture<Message<Order>> handleMessage(CompletableFuture<Message<Order>> message) {
        return message.thenApplyAsync((msg) -> {
            System.out.println("Handling message:  " + msg.getValue());
            return msg;
        });
    }

    @Override
    protected CompletableFuture<Void> setup(PulsarClient client) {
        setLoopTime(0);

        consumer = client.newConsumer(Schema.JSON(Order.class))
                .topic(consumerTopicName)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Latest)
                .subscribeAsync();

        return consumer.thenAccept(this::registerResource);

    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        BacklogDLQConsumer consumer = new BacklogDLQConsumer();
        ShutdownHook.registerForShutdownHook(consumer);
        consumer.run().get();
    }
}