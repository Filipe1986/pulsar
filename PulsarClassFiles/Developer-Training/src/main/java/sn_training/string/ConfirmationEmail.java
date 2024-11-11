package sn_training.string;

import org.apache.pulsar.client.api.*;
import sn_training.Order;
import sn_training.util.AsyncRunner;
import sn_training.util.ShutdownHook;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static sn_training.Config.*;

/**
 * mvn compile exec:java@confirmationEmail
 * 
 * Implementing our first async consumer
 *
 **/
public class ConfirmationEmail extends AsyncRunner {

    CompletableFuture<Consumer<String>> consumer;
    final String consumerTopicName = Topics.ORDER_APPROVED;
    final String subscriptionName = "confirmation_email";

    /**
     * The work loop will receive and process a message one at a time.
     *
     */
    @Override
    protected CompletableFuture<?> workLoop() {
        return consumer.thenCompose((consumer)-> handleMessage(consumer.receiveAsync())
                .thenCompose(consumer::acknowledgeAsync));
    };


    CompletableFuture<Message<String>> handleMessage(CompletableFuture<Message<String>> message) {
        return message.thenApplyAsync((msg) -> {
            System.out.println("Handling message:  " + msg.getValue());
            return msg;
        });
    }

    @Override
    protected CompletableFuture<Void> setup(PulsarClient client) {
        setLoopTime(0);

        consumer = client.newConsumer(Schema.STRING)
                .topic(consumerTopicName)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribeAsync();

        return consumer.thenAccept(this::registerResource);

    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ConfirmationEmail inventoryChecker = new ConfirmationEmail();
        ShutdownHook.registerForShutdownHook(inventoryChecker);
        inventoryChecker.run().get();
    }
}