package sn_training.struct.sharedSubscription;

import org.apache.pulsar.client.api.*;
import sn_training.Order;
import sn_training.struct.DynamicStructProducerFactory;
import sn_training.util.AsyncRunner;
import sn_training.util.ShutdownHook;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static sn_training.Config.StructTopics;


public class InventoryCheckerSharedStructChina extends AsyncRunner {

    CompletableFuture<Consumer<Order>> consumer;
    final String consumerTopicName = StructTopics.ORDER_BACKLOG_CHINA;
    final String subscriptionName = "inventory_checker_shared_struct";
    DynamicStructProducerFactory producerFactory;

    @Override
    protected CompletableFuture<?> workLoop() {
        return consumer.thenCompose((consumer)-> handleMessage(consumer.receiveAsync())
                .thenCompose(consumer::acknowledgeAsync));
    };


    CompletableFuture<Message<Order>> handleMessage(CompletableFuture<Message<Order>> message) {
        return message.thenApplyAsync((msg) -> {
            System.out.println("Handling message:  " + msg.getValue().getUniqueOrderNumber() + " for partition " + msg.getTopicName());

            System.out.println("msg" + msg.getValue());
            String topic = null;


            if(msg.getValue().getQuantity() > 7) {
                topic = StructTopics.ORDER_DECLINED;
            } else {
                topic = StructTopics.ORDER_APPROVED;
            }


            sleep(5000);

            CompletableFuture<MessageId> myFuture = producerFactory.getProducer(topic).thenCompose((producer)
                    -> producer.newMessage().key(msg.getValue().getCountry()).value(msg.getValue()).sendAsync());

            try {
                myFuture.get();
            } catch (ExecutionException | InterruptedException e) {
                e.printStackTrace();
            }

            System.out.println("Published and will acknowledge message " + msg + " to topic " + topic);

            return msg;
        });
    }

    private static void sleep(int milliseconds) {
        try {
            Thread.sleep(new Random().nextInt(milliseconds));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

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
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Latest)
                .subscribeAsync();

        return consumer.thenAccept(this::registerResource);

    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        InventoryCheckerSharedStructChina inventoryChecker = new InventoryCheckerSharedStructChina();
        ShutdownHook.registerForShutdownHook(inventoryChecker);
        inventoryChecker.run().get();
    }
}