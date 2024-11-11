package sn_training.dev_training;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.apache.pulsar.client.api.*;
import sn_training.util.AsyncRunner;
import sn_training.util.ShutdownHook;
import sn_training.Order;
import sn_training.DynamicProducerFactory;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;
import java.net.URL;
import java.net.MalformedURLException;

/**
 * mvn compile exec:java@table_reader
 * 
 * Various ways to read from or trigger actions with TableView
 *
 **/
public class TableReader {

    //our table view object
    TableView<Long> tv;

    final String topicName = "persistent://student01/developer/my_table_view";
    final String subscriptionName = "table_reader_by_country";

    public static String PULSAR_URL = "pulsar+ssl://pc-f317f9eb.gcp-shared-usce1.g.snio.cloud:6651";

    private static String ISSUER_URL = "https://auth.streamnative.cloud";
    private static String AUDIENCE = "urn:sn:pulsar:o-mj3r8:train";
    private static String CREDENTIALS_URL = "file:///config/workspace/o-mj3r8-student31-05032024.json";

    PulsarClient pulsarClient;

    //TableReader constructer
    TableReader() {
        //create pulsar client
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

    //create table for each key once, take action from each key
    
    tv.forEach((key,value) -> {
        System.out.println("Key: " + key + " Value: " + value);
    });
    
    
    //create table for each key and take action, follow up with listening for updates and taking action
    /*
    tv.forEachAndListen((key,value) -> {
        System.out.println("Key: " + key + " Value: " + value);
    });
    */

    System.out.println("exited forEach or forEachAndListen");
    return;
}

//get value of specific key at a given time
private Long getValue(String myCountry) {
    return tv.get(myCountry);
}

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        TableReader myPulsarTableView = new TableReader();

        //test getValue() check latest value
        
        String myCountry = "US";
        Long myValue = myPulsarTableView.getValue(myCountry);
        System.out.println(" Value of " + myCountry + " using get: " + myValue);
        

        //execute doWork() method
        /*
        myPulsarTableView.doWork();
        */

        return;
    }
}