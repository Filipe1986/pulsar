package sn_training.util;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.io.Closeable;
import java.util.ArrayList;

/**
 * This base class is just to slim down the boilerplate in each class.
 *
 * It takes care of:
 * - creating the pulsar client
 * - has a method for registering closeable resources, like producers, consumers, etc
 * - implements a work loop for periodic messaging sending
 */
public abstract class SyncRunner implements Shutdownable {
    protected boolean running = false;
    protected PulsarClient client;
    protected int loopTimeMills = 1000;

    protected ArrayList<Closeable> toClose;

    public void run() throws Exception {
        System.out.println("Starting " + this.getClass().getName());
        running = true;
        toClose = new ArrayList<>();
        client = PulsarClientFactory.createClient();
        setup(client);
        doWork();
        cleanup();
    }

    protected void doWork() throws InterruptedException, PulsarClientException {
        while (running) {
            workLoop();
            if (loopTimeMills > 0) {
                Thread.sleep(loopTimeMills);
            }
        }
    }

    protected void registerResource(Closeable c) {
        this.toClose.add(c);
    }
    protected void setLoopTime(int loopTimeInMills) {
        this.loopTimeMills = loopTimeInMills;
    }

    protected abstract void workLoop() throws PulsarClientException;
    protected abstract void setup(PulsarClient client) throws PulsarClientException;

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