package org.example.config;

public class Config {
    public static final String CREDENTIALS_URL = "file:///Users/goncalvesf2/code/Pulsar/GettingStartedWithPulsar/src/main/resources/o-mj3r8-student129-10312024.json";


    public static final String ISSUER_URL = "https://auth.streamnative.cloud/";
    public static final  String AUDIENCE = "urn:sn:pulsar:o-mj3r8:train";


    public static final String TOPIC_NAME = "persistent://student129/developer";
    public static final String JSON_TOPIC_NAME = TOPIC_NAME + "/jsontopic";
    public static final String AVRO_TOPIC_NAME = TOPIC_NAME + "/avrotopic";
    public static final String STRING_TOPIC_NAME = TOPIC_NAME + "/stringtopic";
    public static final String SETUP_TOPIC_NAME = TOPIC_NAME + "/setuptopic";
    public static final String DATA_GEN_INPUT_TOPIC_NAME = TOPIC_NAME + "/datageninput";

}
