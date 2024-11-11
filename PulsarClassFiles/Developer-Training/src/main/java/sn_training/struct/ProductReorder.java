package sn_training.struct;

import org.apache.pulsar.client.api.*;
import sn_training.Config;
import sn_training.Order;
import sn_training.util.AsyncRunner;
import sn_training.util.ShutdownHook;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * mvn compile exec:java@productReorder
 * 
 * Implementing our first async consumer
 *
 **/
public class ProductReorder extends AsyncRunner {

    CompletableFuture<Consumer<Order>> consumer;
    //final String consumerTopicName = Config.StructTopics.ORDER_APPROVED;
    List<String> consumerTopicList = List.of(Config.StructTopics.ORDER_APPROVED, Config.StructTopics.ORDER_DECLINED);
    final String subscriptionName = "productReorder";

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
            System.out.println("Reorder Items:  " + msg.getValue().getUniqueOrderNumber() + " for quantity " + msg.getValue().getQuantity() + " from " + msg.getTopicName());
            return msg;
        });
    }

    @Override
    protected CompletableFuture<Void> setup(PulsarClient client) {
        setLoopTime(0);

        consumer = client.newConsumer(Schema.JSON(Order.class))
                .topics(consumerTopicList)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Latest)
                .subscribeAsync();

        return consumer.thenAccept(this::registerResource);

    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ProductReorder productReorder = new ProductReorder();
        ShutdownHook.registerForShutdownHook(productReorder);
        productReorder.run().get();
    }
}