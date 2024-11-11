package org.example;

import java.net.URL;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;
//pulsar-io/data-generator/src/main/java/org/apache/pulsar/io/datagenerator/Person.java
import org.apache.pulsar.io.datagenerator.Person;
import org.example.config.Config;

public class SNConsumerDatagen {

    public static void main(String[] args) throws Exception {

        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar+ssl://pc-f317f9eb.gcp-shared-usce1.g.snio.cloud:6651")
                .authentication(
                        AuthenticationFactoryOAuth2.clientCredentials(new URL(Config.ISSUER_URL),
                                new URL(Config.CREDENTIALS_URL),
                                Config.AUDIENCE))
                .build();

        Consumer consumer = client.newConsumer(Schema.JSON(Person.class))
                .topic(Config.DATA_GEN_INPUT_TOPIC_NAME)
                .subscriptionName("mysubscription")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        for (int i = 0; i < 1000; i++) {
            Message<Person> msg = consumer.receive();
            consumer.acknowledge(msg);
            Person person = msg.getValue();
            System.out.println("Receive message " + person.getFirstName() + " " + person.getLastName() + " " + person.getTelephoneNumber());
        }
        consumer.close();
        client.close();
    }
}