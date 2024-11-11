package sn_training.test_setup;

import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;

import java.net.MalformedURLException;
import java.net.URL;

import static sn_training.ConfigLocal.*;

/**
 * mvn compile exec:java@test_setup
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
public class LocalTestSetup {
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

    public static void main(String[] args) {

        PulsarClient client = createClient();

        Producer<String> producer = null;
        try {
            producer = client.newProducer(Schema.STRING).topic(SETUP_TOPIC_NAME).create();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }

            String message = "Test message";
            try {
                producer.newMessage().value(message).send();
                System.out.println("Published message " + message + " synchronously to topic " + SETUP_TOPIC_NAME);
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }

        try {
            producer.close();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }

        System.out.println("Client closed: " + client.isClosed());
        try {
            client.close();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
        System.out.println("Client closed: " + client.isClosed());

        System.out.println("Exiting");
    }
}