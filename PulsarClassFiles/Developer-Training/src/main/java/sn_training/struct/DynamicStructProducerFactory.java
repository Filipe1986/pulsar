package sn_training.struct;

import org.apache.pulsar.client.api.*;
import sn_training.Order;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class DynamicStructProducerFactory implements Closeable {

    public final Map<String, CompletableFuture<Producer<Order>>> producerCache;

    public final PulsarClient client;

    public DynamicStructProducerFactory(PulsarClient client) {
        this.client = client;
        this.producerCache = new HashMap<>();
    }
    
    public CompletableFuture<Producer<Order>> getProducer(String topicName) {
        return producerCache.computeIfAbsent(topicName, (tn) -> {
            System.out.println("Creating new producer with topic " + tn);
            return client.newProducer(Schema.JSON(Order.class))
                    .batcherBuilder(BatcherBuilder.KEY_BASED)
                    .topic(tn)
                    .createAsync();
        });
    }

    @Override
    public void close() throws IOException {
        for (CompletableFuture<Producer<Order>> producerFuture : producerCache.values()) {
            try {
                producerFuture.get().close();
            } catch (PulsarClientException | InterruptedException | ExecutionException e) {
                throw new IOException(e);
            }
        }
    }
}