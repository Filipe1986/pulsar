package sn_training.struct;

import org.apache.pulsar.client.api.*;
import sn_training.Order;
import sn_training.util.AsyncRunner;
import sn_training.util.ShutdownHook;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static sn_training.Config.StructTopics;

/**
 * mvn compile exec:java@inventory_checker_china
 * 
 * Implementing our first async consumer
 *
 **/
public class EmailScheduler extends AsyncRunner {

    CompletableFuture<Consumer<Order>> consumer;
    final String consumerTopicName = StructTopics.ORDER_APPROVED;
    final String subscriptionName = "email_scheduler";
    DynamicStructProducerFactory producerFactory;

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
            System.out.println("Handling message:  " + msg.getValue().getUniqueOrderNumber());

            Long deliveryTopic = Instant.now().plusSeconds(20).toEpochMilli();
            CompletableFuture<MessageId> myFuture = producerFactory.getProducer(StructTopics.SCHEDULED_MARKETING_EMAIL)
                    .thenCompose((producer) -> producer.newMessage().deliverAt(deliveryTopic).value(msg.getValue()).sendAsync());

            try {
                myFuture.get();
            } catch (ExecutionException | InterruptedException e) {
                e.printStackTrace();
            }

            System.out.println("Published and will acknowledge message " + msg.getValue() + " to topic: " + StructTopics.SCHEDULED_MARKETING_EMAIL);

            return msg;
        });
    }

    /**
     * setup client to consume messages as fast as possible
     * see newConsumer builder for changes this goes through during the course
     */
    @Override
    protected CompletableFuture<Void> setup(PulsarClient client) {

        setLoopTime(0);
        producerFactory = new DynamicStructProducerFactory(client);
        registerResource(producerFactory);


        consumer = client.newConsumer(Schema.JSON(Order.class))
                .topic(consumerTopicName)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Latest)
                .subscribeAsync();

        return consumer.thenAccept(this::registerResource);

    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        EmailScheduler emailScheduler = new EmailScheduler();
        ShutdownHook.registerForShutdownHook(emailScheduler);
        emailScheduler.run().get();
    }
}