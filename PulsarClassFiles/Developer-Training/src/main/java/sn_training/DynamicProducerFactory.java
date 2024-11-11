package sn_training;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.io.Closeable;
import java.util.HashMap;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.logging.log4j.core.config.OrderComparator;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.BatcherBuilder;

public class DynamicProducerFactory implements Closeable {
    // this is our cache of created topics, with the key of the map being the topic name
    // and the value of the map being a CompletableFuture instance
    // We cache the future to make the interface consistent for both new producers
    // and cached producers

    public final Map<String, CompletableFuture<Producer<String>>> producerCache;
    
    public final PulsarClient client;

    public DynamicProducerFactory(PulsarClient client) {
        this.client = client;
        this.producerCache = new HashMap<>();
    }
    
    public CompletableFuture<Producer<String>> getProducer(String topicName) {
        return producerCache.computeIfAbsent(topicName, (tn) -> {
            System.out.println("Creating new producer with topic " + tn);
            return client.newProducer(Schema.STRING).topic(tn).createAsync();
        });
    }

    @Override
    public void close() throws IOException {
        for (CompletableFuture<Producer<String>> producerFuture : producerCache.values()) {
            try {
                producerFuture.get().close();
            } catch (PulsarClientException | InterruptedException | ExecutionException e) {
                throw new IOException(e);
            }
        }
    }
}