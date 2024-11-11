package org.example.json;

import java.net.URL;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;
import org.example.config.Config;
import org.example.model.SensorReading;

public class SNConsumerJSON {

    public static void main(String[] args) throws Exception
    {

        String audience = "urn:sn:pulsar:o-mj3r8:train";

        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar+ssl://pc-f317f9eb.gcp-shared-usce1.g.snio.cloud:6651")
                .authentication(
                        AuthenticationFactoryOAuth2.clientCredentials(new URL(Config.ISSUER_URL),
                                new URL(Config.CREDENTIALS_URL),
                                audience))
                .build();

        Consumer consumer = client.newConsumer(Schema.JSON(SensorReading.class))
                .topic(Config.JSON_TOPIC_NAME)
                .subscriptionName("mysubscription")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        for (int i = 0; i < 1000; i++) {
            Message<SensorReading> msg = consumer.receive();
            consumer.acknowledge(msg);
            SensorReading myReading = msg.getValue();
            System.out.println("Receive message " + myReading.getSensorName() + " " + myReading.getValue());
        }

        consumer.close();
        client.close();
    }
}