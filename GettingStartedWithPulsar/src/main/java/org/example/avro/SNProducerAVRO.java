package org.example.avro;

import java.net.URL;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.example.config.Config;
import org.example.model.SensorReading;

public class SNProducerAVRO {

    public static void main(String[] args) throws Exception {

        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar+ssl://pc-f317f9eb.gcp-shared-usce1.g.snio.cloud:6651")
                .authentication(
                        AuthenticationFactoryOAuth2.clientCredentials(new URL(Config.ISSUER_URL),
                                new URL(Config.CREDENTIALS_URL),
                                Config.AUDIENCE))
                .build();

        Producer<SensorReading> producer = client.newProducer(AvroSchema.of(SensorReading.class))
                .topic(Config.AVRO_TOPIC_NAME)
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