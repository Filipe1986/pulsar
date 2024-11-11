package sn_training.test_setup;

import java.net.URL;
import java.net.MalformedURLException;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;

import static sn_training.Config.*;

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
public class TestSetup {

    // the URL to use for local standalone Pulsar



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