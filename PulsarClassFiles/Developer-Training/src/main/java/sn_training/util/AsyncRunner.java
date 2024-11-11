package sn_training.util;

import org.apache.pulsar.client.api.PulsarClient;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * This base class is just to slim down the boilerplate in each class.
 *
 * It takes care of:
 * - creating the pulsar client
 * - has a method for registering closeable resources, like producers, consumers, etc
 * - implements a work loop for periodic messaging sending
 */
public abstract class AsyncRunner implements Shutdownable {
    protected boolean running = false;
    protected PulsarClient client;
    protected int loopTimeMills = 1000;

    protected ArrayList<Closeable> toClose;

    public CompletableFuture<Void> run() {
        System.out.println("Starting " + this.getClass().getName());
        running = true;
        toClose = new ArrayList<>();
        client = PulsarClientFactory.createClient();
        return setup(client).thenCompose((res) -> doWork()).whenComplete((res, err) -> {
            if (err != null) {
                System.err.println("Had an exception, failing!");
                err.printStackTrace();
            }
            try {
                System.out.println("cleaning up");
                cleanup();
            } catch (Exception e) {
                throw new CompletionException("exception while cleaning up", e);
            }
        });
    }

    protected CompletableFuture<Void> doWork() {
        return CompletableFuture.runAsync(() -> {
            while (running) {
                try {
                    workLoop().get(); //hold until workLoop complete
                    if (loopTimeMills > 0) {
                        Thread.sleep(loopTimeMills);
                    }
                } catch (InterruptedException | ExecutionException e) {
                    throw new CompletionException(e);
                }
            }
        });
    }

    protected void registerResource(Closeable c) {
        this.toClose.add(c);
    }
    protected void setLoopTime(int loopTimeInMills) {
        this.loopTimeMills = loopTimeInMills;
    }

    protected abstract CompletableFuture<?> workLoop();
    protected abstract CompletableFuture<Void> setup(PulsarClient client);

    @Override
    public void shutdown() {
        running = false;
    }

    @Override
    public void cleanup() throws Exception {
        for (Closeable closeable : toClose) {
            closeable.close();
        }
        client.close();
    }
}