package org.example;

import java.net.URL;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;
import org.example.config.Config;

public class SNProducer {

    public static void main(String[] args) throws Exception {


        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar+ssl://pc-f317f9eb.gcp-shared-usce1.g.snio.cloud:6651")
                .authentication(
                        AuthenticationFactoryOAuth2.clientCredentials(new URL(Config.ISSUER_URL),
                                new URL(Config.CREDENTIALS_URL),
                                Config.AUDIENCE))
                .build();


        Producer<byte[]> producer = client.newProducer()
                .topic(Config.SETUP_TOPIC_NAME)
                // convert to multi-tenancy, use non-super admin account that only has produce and consume in this namespace
                //.topicName("persistent://mytenant/mynamespace/bytetopic")
                //using send(), if switch to sendAsync() when using keys, be sure to use KEY_BASED batching for correct message routing
                //.batcherBuilder(BatcherBuilder.KEY_BASED)
                .create();

        for (int i = 0; i < 30; i++) {

            String message = "my-message-" + i;

            //add key to test Key_Shared
            /*
            String myKey  = null;
            switch (i % 6) {
                case 0:
                    myKey = "cat";
                    break;
                case 1:
                    myKey = "dog";
                    break;
                case 2:
                    myKey = "mouse";
                    break;
                case 3:
                    myKey = "squirrel";
                    break;
                case 4:
                    myKey = "lion";
                    break;
                case 5:
                    myKey = "frog";
                    break;
            }
            */

            MessageId msgID = producer.send(message.getBytes());
            System.out.println("Publish " + "my-message-" + i + " and message ID " + msgID);

            //use newMessage() when adding key
            //MessageId msgID = producer.newMessage().key(myKey).value(message.getBytes()).send();
            //System.out.println("Publish " + "my-message-" + i + " and message ID " + msgID + " with key " + myKey);
        }
        producer.close();
        client.close();
    }
}