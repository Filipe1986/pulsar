package sn_training.struct;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.apache.pulsar.client.api.*;
import sn_training.Config;
import sn_training.util.AsyncRunner;
import sn_training.util.ShutdownHook;
import sn_training.Order;
import sn_training.DynamicProducerFactory;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;
import java.net.URL;
import java.net.MalformedURLException;

import static sn_training.Config.*;

/**
 * mvn compile exec:java@table_reader
 * 
 * Various ways to read from or trigger actions with TableView
 *
 **/
public class TableReader {

    //our table view object
    TableView<Long> tv;

    final String topicName = Config.StructTopics.MY_TABLE_VIEW;
    final String subscriptionName = "table_reader_by_country";


    PulsarClient pulsarClient;

    TableReader() {
        System.out.println("Entering constructor");
        try {
            pulsarClient = PulsarClient.builder()
                .serviceUrl(PULSAR_URL)
                .authentication(getAuth())
                .allowTlsInsecureConnection(false)
                .enableTlsHostnameVerification(true)
                .build();
        } catch (PulsarClientException e) {
        }
        System.out.println("Client created");

        //create table view
        try {
            tv = pulsarClient.newTableView(Schema.INT64)
                .topic(topicName)
                .create();
        } catch (PulsarClientException e) {
            
        }
        System.out.println("TableView reader created");
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

void doWork() {
    tv.forEachAndListen((key,value) -> {
        System.out.println("Key: " + key + " Value: " + value);
    });

}

private Long getValue(String myCountry) {
    return tv.get(myCountry);
}
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        TableReader myPulsarTableView = new TableReader();
        myPulsarTableView.doWork();
    }
}