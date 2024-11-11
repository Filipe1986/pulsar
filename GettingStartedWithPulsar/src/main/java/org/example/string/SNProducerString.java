package org.example.string;

import java.net.URL;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;
import org.example.config.Config;

public class SNProducerString {

    public static void main(String[] args) throws Exception {

        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar+ssl://pc-f317f9eb.gcp-shared-usce1.g.snio.cloud:6651")
                .authentication(
                        AuthenticationFactoryOAuth2.clientCredentials(new URL(Config.ISSUER_URL),
                                new URL(Config.CREDENTIALS_URL),
                                Config.AUDIENCE))
                .build();

        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(Config.STRING_TOPIC_NAME)
                .create();

        for (int i = 0; i < 10; i++) {
            String message = "my-message-" + i;
            MessageId msgID = producer.send(message);
            System.out.println("Publish " + "my-message-" + i + " and message ID " + msgID);
        }
        producer.close();
        client.close();
    }
}