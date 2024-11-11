package org.example;

import java.net.URL;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;
import org.example.config.Config;
//import java.util.Random;

public class SNConsumer {

    public static void main(String[] args) throws Exception {

        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar+ssl://pc-f317f9eb.gcp-shared-usce1.g.snio.cloud:6651")
                .authentication(
                        AuthenticationFactoryOAuth2.clientCredentials(new URL(Config.ISSUER_URL),
                                new URL(Config.CREDENTIALS_URL),
                                Config.AUDIENCE ))
                .build();

        Consumer consumer = client.newConsumer()
                .topic(Config.SETUP_TOPIC_NAME)
                // convert to multi-tenancy, use non-super admin account that only has produce and consume in this namespace
                //.topic("persistent://mytenant/mynamespace/bytetopic")
                .subscriptionName("subscription")
                //introduce all four subscription types, default is Exclusive as viewable in the UI
                //test Exclusive, Failover, and Shared
                //will test Key_Shared later when discussing topic partitioning and keys
                //retest Exclusive, Failover, and Shared with partitioned topic since the behavior is sligthly different
                //e.g. Exclusive is in-order on a per partition basis
                .subscriptionType(SubscriptionType.Exclusive)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                //default receiverQueueSize is 1000, reduce to 1 for testing only to watch Shared Subscription distribute load
                //.receiverQueueSize(1)
                .subscribe();

        //Random random = new Random();

        for (int i = 0; i < 1000; i++) {
            Message<String> msg = consumer.receive();

            //random wait to simulate work when demonstrating Shared subscription with small receive queue size
            //System.out.println("Received messaged " + new String(msg.getData()));
            //Thread.sleep(random.nextInt(10000));
            consumer.acknowledge(msg);

            System.out.println("Processed message " + new String(msg.getData()));
            //System.out.println("Processed message " + new String(msg.getData()) + " " + msg.getKey() + " from topic " + msg.getTopicName());
        }

        consumer.close();
        client.close();
    }
}