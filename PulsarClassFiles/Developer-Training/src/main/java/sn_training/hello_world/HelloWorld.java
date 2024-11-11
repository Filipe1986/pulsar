package sn_training.hello_world;

import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;

import java.net.MalformedURLException;
import java.net.URL;

import static sn_training.Config.*;

/**
 * mvn compile exec:java@hello_world
 * 
 * synchronous client, and producer
 * 
 * 1. connects to streamnative cloud
 * 2. creates pulsar client synchronously
 * 3. create pulsar producer synchronously
 * 4. send one message
 * 5. close producer
 * 6. close client
 **/
public class HelloWorld {


    public static PulsarClient createClient() {
        PulsarClient pulsarClient = null;
        try {
            pulsarClient = PulsarClient.builder()
                    .serviceUrl(PULSAR_URL)
                    .authentication(getAuth())
                    .allowTlsInsecureConnection(false)
                    .enableTlsHostnameVerification(true)
                    .build();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
        return pulsarClient;
    }

    private static Authentication getAuth() {
        try {
            return AuthenticationFactoryOAuth2.clientCredentials(
                        new URL(ISSUER_URL),
                        new URL(CREDENTIALS_URL),
                        AUDIENCE);
        } catch (MalformedURLException e) {
            return null;
        }
    }

    public static void main(String[] args) throws PulsarClientException {

        PulsarClient client = createClient();

        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(STRING_TOPIC_NAME)
                .create();

        String value = "Hello World!";
        producer.newMessage().value(value).send();
        System.out.println("Published message "+ value + " to topic: " + STRING_TOPIC_NAME);

        producer.close();
        System.out.println("Producer closed: " + client.isClosed());


        client.close();
        System.out.println("Client closed: " + client.isClosed());
        System.out.println("Exiting");
    }
}