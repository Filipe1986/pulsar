package sn_training.struct.sharedSubscription;

import org.apache.pulsar.client.api.*;
import sn_training.Config;
import sn_training.Order;
import sn_training.util.AsyncRunner;
import sn_training.util.ShutdownHook;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class MarketingEmail extends AsyncRunner {

    CompletableFuture<Consumer<Order>> consumer;
    final String consumerTopicName = Config.StructTopics.SCHEDULED_MARKETING_EMAIL;
    final String subscriptionName = "marketing_email";

    @Override
    protected CompletableFuture<?> workLoop() {
        return consumer.thenCompose((consumer)-> handleMessage(consumer.receiveAsync())
                .thenCompose(consumer::acknowledgeAsync));
    };


    CompletableFuture<Message<Order>> handleMessage(CompletableFuture<Message<Order>> message) {
        return message.thenApplyAsync((msg) -> {
            System.out.printf("Market Email message: %s read\n", msg.getValue() );
            return msg;
        });
    }

    @Override
    protected CompletableFuture<Void> setup(PulsarClient client) {
        setLoopTime(0);

        consumer = client.newConsumer(Schema.JSON(Order.class))
                .topic(consumerTopicName)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Latest)
                .subscribeAsync();

        return consumer.thenAccept(this::registerResource);

    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        MarketingEmail marketingEmail = new MarketingEmail();
        ShutdownHook.registerForShutdownHook(marketingEmail);
        marketingEmail.run().get();
    }
}