package sn_training.struct;

import org.apache.pulsar.client.api.*;
import sn_training.DynamicProducerFactory;
import sn_training.Order;
import sn_training.util.AsyncRunner;
import sn_training.util.ShutdownHook;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static sn_training.Config.*;

/**
 * mvn compile exec:java@inventory_checker_china
 *
 *
 **/
public class InventoryCheckerStructUS extends AsyncRunner {

    CompletableFuture<Consumer<Order>> consumer;
    final String consumerTopicName = StructTopics.ORDER_BACKLOG_US;
    final String subscriptionName = "inventory_checker";
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
        return message.thenApplyAsync((msg) -> { //do something with message once it's obtained by consumer, then pass back to let consumer acknowledge it once we've confirmed we're done
            System.out.println("Handling message:  " + msg.getValue() + " for partition " + msg.getTopicName());

            String topic = null;
            if(msg.getValue().getQuantity() > 7) {
                topic = StructTopics.ORDER_DECLINED;
            } else {
                topic = StructTopics.ORDER_APPROVED;
            }

            //get producer from DynamicProducerFactory and then send message
            CompletableFuture<MessageId> myFuture = producerFactory.getProducer(topic).thenCompose((producer) -> {
                return producer.newMessage().key(msg.getValue().getCountry()).value(msg.getValue()).sendAsync();
            });

            try {
                myFuture.get(); //blocking until sendAsync completes
            } catch (ExecutionException | InterruptedException e) {
                e.printStackTrace();
            }

            System.out.println("Published and will acknowledge message " + msg.getValue() + " to topic " + topic);

            return msg;
        });
    }

    /**
     * setup client to consume messages as fast as possible
     * see newConsumer builder for changes this goes through during the course
     */
    @Override
    protected CompletableFuture<Void> setup(PulsarClient client) {
        // consume as fast as possible
        setLoopTime(0);

        //in addition to creating the consumer, let's create the producerFactory since we want to write to two other topics
        producerFactory = new DynamicStructProducerFactory(client); //need DynamicProducerFactory method
        registerResource(producerFactory);

        //TODO create and register consumer, return completable future
        consumer = client.newConsumer(Schema.JSON(Order.class))
                .topic(consumerTopicName)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribeAsync();

        return consumer.thenAccept(this::registerResource);

    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        InventoryCheckerStructUS inventoryChecker = new InventoryCheckerStructUS();
        ShutdownHook.registerForShutdownHook(inventoryChecker);
        inventoryChecker.run().get();
    }
}