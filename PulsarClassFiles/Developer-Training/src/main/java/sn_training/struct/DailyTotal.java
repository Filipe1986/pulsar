package sn_training.struct;

import org.apache.pulsar.client.api.*;
import sn_training.Config;
import sn_training.Order;
import sn_training.util.AsyncRunner;
import sn_training.util.ShutdownHook;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class DailyTotal extends AsyncRunner {

    CompletableFuture<Consumer<Order>> consumer;
    final String consumerTopicName = Config.StructTopics.ORDER_APPROVED;
    final String subscriptionName = "daily_total";

//    Long currentSum = 0L;
//    Long startTimeOfCurrentSum = 0L;
    private class CurrentValue {
        Long time = 0L;
        Long sum = 0L;
    }

    private Map<String, CurrentValue> totalCache;

    @Override
    protected CompletableFuture<?> workLoop() {
        return consumer.thenCompose((consumer)-> handleMessage(consumer.receiveAsync())
                .thenCompose(consumer::acknowledgeAsync));
    }


    CompletableFuture<Message<Order>> handleMessage(CompletableFuture<Message<Order>> message) {
        return message.thenApplyAsync((msg) -> {
            System.out.println("Handling message:  " + msg.getValue().getUniqueOrderNumber() + " for quantity " + msg.getValue().getQuantity() + " from " + msg.getValue().getCountry());

            Long timeMessage = msg.getPublishTime();
            Long startTimeOfCurrentMessage = Instant.ofEpochMilli(timeMessage).truncatedTo(ChronoUnit.MINUTES).toEpochMilli();

            CurrentValue myCurrent = totalCache.computeIfAbsent(msg.getValue().getCountry(), (country) -> {
                System.out.println("New country: " + country);
                return new CurrentValue();

            });

            if(myCurrent.equals(startTimeOfCurrentMessage)){
                myCurrent.sum = myCurrent.sum + msg.getValue().getQuantity();

            }else {
                myCurrent.sum = (long) msg.getValue().getQuantity();
                myCurrent.time = startTimeOfCurrentMessage;
            }

            System.out.println("Current sum: " + myCurrent.sum + " for time: " + myCurrent.time + " for country: " + msg.getValue().getCountry());
            return msg;
        });
    }

    @Override
    protected CompletableFuture<Void> setup(PulsarClient client) {
        setLoopTime(0);

        totalCache = new HashMap<>();
        Long timeToReset = Instant.now().truncatedTo(ChronoUnit.MINUTES).minus(2, ChronoUnit.MINUTES).toEpochMilli() ;

        consumer = client.newConsumer(Schema.JSON(Order.class))
                .topic(consumerTopicName)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Latest)
                .subscribeAsync();

        consumer.thenCompose(consumer -> {
            // set the consumer to start to read from a time in the past
            return consumer.seekAsync(timeToReset);
        });
        return consumer.thenAccept(this::registerResource);

    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        DailyTotal confirmationStructEmail = new DailyTotal();
        ShutdownHook.registerForShutdownHook(confirmationStructEmail);
        confirmationStructEmail.run().get();
    }
}