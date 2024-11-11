package org.example.string;

import java.net.URL;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;
import org.example.config.Config;

public class SNConsumerString {

    public static void main(String[] args) throws Exception {

        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar+ssl://pc-f317f9eb.gcp-shared-usce1.g.snio.cloud:6651")
                .authentication(
                        AuthenticationFactoryOAuth2.clientCredentials(new URL(Config.ISSUER_URL),
                                new URL(Config.CREDENTIALS_URL),
                                Config.AUDIENCE))
                .build();

        Consumer consumer = client.newConsumer(Schema.STRING)
                .topic(Config.STRING_TOPIC_NAME)
                .subscriptionName("mysubscription")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        for (int i = 0; i < 10; i++) {
            Message<byte[]> msg = consumer.receive();
            consumer.acknowledge(msg);
            System.out.println("Receive message " + msg.getValue());
        }

        consumer.close();
        client.close();
    }
}