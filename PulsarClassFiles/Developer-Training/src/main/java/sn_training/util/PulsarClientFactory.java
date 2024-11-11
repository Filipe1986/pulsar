package sn_training.util;

import java.net.URL;
import java.net.MalformedURLException;

import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;
import sn_training.Config;

import static sn_training.Config.*;

/**
 * provided for use throughout course to create Pulsar Client whenever needed
 **/
public class PulsarClientFactory {

    public static PulsarClient createClient() {
        PulsarClient pulsarClient = null;
        try {
            pulsarClient = PulsarClient.builder()
                    .serviceUrl(Config.PULSAR_URL)
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
}