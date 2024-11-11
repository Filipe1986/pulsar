package sn_training.string;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.Consumer;
import sn_training.util.AsyncRunner;
import sn_training.util.ShutdownHook;
import sn_training.DynamicProducerFactory;

import static sn_training.Config.*;

/**
 * mvn compile exec:java@inventory_checker_china
 * 
 * Implementing our first async consumer
 *
 **/
public class InventoryCheckerChina extends AsyncRunner {

    CompletableFuture<Consumer<String>> consumer;
    final String consumerTopicName = Topics.ORDER_BACKLOG_CHINA;
    final String subscriptionName = "inventory_checker";
    DynamicProducerFactory producerFactory;

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
        return message.thenApplyAsync((msg) -> { //do something with message once it's obtained by consumer, then pass back to let consumer acknowledge it once we've confirmed we're done
            System.out.println("Handling message:  " + msg.getValue());

            String topic = null;
            if(msg.getValue().contains("2")) {
                topic = Topics.ORDER_DECLINED;
            } else {
                topic = Topics.ORDER_APPROVED;
            }

            //get producer from DynamicProducerFactory and then send message
            CompletableFuture<MessageId> myFuture = producerFactory.getProducer(topic).thenCompose((producer) -> {
                return producer.newMessage().value(msg.getValue()).sendAsync();
            });

            try {
                myFuture.get(); //blocking until sendAsync completes
            } catch (ExecutionException | InterruptedException e) {
                e.printStackTrace();
            }

            System.out.println("Published and will acknowledge message " + msg.getValue() + " to topic " + topic);

            return msg; //return original message to allow consumer to acknowledge using acknowledgeAsync
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
        producerFactory = new DynamicProducerFactory(client); //need DynamicProducerFactory method
        registerResource(producerFactory);

        //TODO create and register consumer, return completable future
        consumer = client.newConsumer(Schema.STRING)
                .topic(consumerTopicName)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribeAsync();

        return consumer.thenAccept(this::registerResource);

    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        InventoryCheckerChina inventoryChecker = new InventoryCheckerChina();
        ShutdownHook.registerForShutdownHook(inventoryChecker);
        inventoryChecker.run().get();
    }
}