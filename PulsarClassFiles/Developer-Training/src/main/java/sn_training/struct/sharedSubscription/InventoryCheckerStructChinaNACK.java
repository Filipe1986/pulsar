package sn_training.struct.sharedSubscription;

import org.apache.pulsar.client.api.*;
import sn_training.Order;
import sn_training.struct.DynamicStructProducerFactory;
import sn_training.util.AsyncRunner;
import sn_training.util.ShutdownHook;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static sn_training.Config.*;

public class InventoryCheckerStructChinaNACK extends AsyncRunner {

    CompletableFuture<Consumer<Order>> consumer;
    final String consumerTopicName = StructTopics.ORDER_BACKLOG_CHINA;
    final String subscriptionName = "inventory_checker_shared_struct";
    DynamicStructProducerFactory producerFactory;

    @Override
    protected CompletableFuture<?> workLoop() {
        return consumer.thenCompose((consumer)-> handleMessageWithNack(consumer.receiveAsync(), consumer)
                .handle((msg, ex) -> {
                    if (ex != null) {
                        System.out.println("Message was already nacked, no need to ack");
                        CompletableFuture<Void> future = new CompletableFuture<>();
                        future.complete(null);
                        return future;
                    }
                    System.out.println("Message Acknowledged");
                    return consumer.acknowledgeAsync(msg);
                }));
    };


    CompletableFuture<Message<Order>> handleMessageWithNack(CompletableFuture<Message<Order>> message, Consumer<Order> consumer) {
        return message.thenApplyAsync((msg) -> {
            System.out.println("Handling message:  " + msg.getValue().getUniqueOrderNumber() + " with quantity " + msg.getValue().getQuantity());

            String topic = (msg.getValue().getQuantity() > 7) ? StructTopics.ORDER_DECLINED : StructTopics.ORDER_APPROVED;
            try {
                if (msg.getValue().getQuantity() == 0) {
                    System.out.println("RedeliveryCount: " + msg.getRedeliveryCount());
                    throw new RuntimeException("Invalid quantity");
                }

                //sleep(5000);

                CompletableFuture<MessageId> myFuture = producerFactory.getProducer(topic).thenCompose((producer)
                        -> producer.newMessage().key(msg.getValue().getCountry()).value(msg.getValue()).sendAsync());

                myFuture.get();

            }catch (ExecutionException | InterruptedException e) {
                // for myFuture.get()
                e.printStackTrace();
            } catch (RuntimeException e) {
                // Invalid quantity
                System.out.println("NACKing message " + msg.getValue() + " with quantity " + msg.getValue().getQuantity());
                consumer.negativeAcknowledge(msg);
                throw e;
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

                .deadLetterPolicy(DeadLetterPolicy
                        .builder()
                        .maxRedeliverCount(2)
                        .deadLetterTopic(StructDLQTopics.ORDER_BACKLOG_CHINA)
                        .build())
                .negativeAckRedeliveryDelay(2, TimeUnit.SECONDS)


                .subscribeAsync();

        return consumer.thenAccept(this::registerResource);

    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        InventoryCheckerStructChinaNACK inventoryChecker = new InventoryCheckerStructChinaNACK();
        ShutdownHook.registerForShutdownHook(inventoryChecker);
        inventoryChecker.run().get();
    }
}