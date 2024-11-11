package org.example.json;

import java.net.URL;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;
import org.example.config.Config;
import org.example.model.SensorReading;

public class SNProducerJSON {

    public static void main(String[] args) throws Exception {


        String audience = "urn:sn:pulsar:o-mj3r8:train";

        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar+ssl://pc-f317f9eb.gcp-shared-usce1.g.snio.cloud:6651")
                .authentication(
                        AuthenticationFactoryOAuth2.clientCredentials(new URL(Config.ISSUER_URL),
                                new URL(Config.CREDENTIALS_URL),
                                audience))
                .build();

        Producer<SensorReading> producer = client.newProducer(Schema.JSON(SensorReading.class))
                .topic(Config.JSON_TOPIC_NAME)
                .create();

        for (int i = 0; i < 10; i++) {
            SensorReading myReading = new SensorReading("mysensor", i);
            MessageId msgID = producer.newMessage().value(myReading).send();
            System.out.println("Publish " + myReading.getSensorName() + " " + myReading.getValue() + " and message ID " + msgID);
        }

        producer.close();
        client.close();
    }
}