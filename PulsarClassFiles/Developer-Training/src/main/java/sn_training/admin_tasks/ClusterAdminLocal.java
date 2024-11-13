package sn_training.admin_tasks;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.schema.SchemaInfo;
import sn_training.Config;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static sn_training.ConfigLocal.*;

/**
 * mvn compile exec:java@cluster_admin
 * <p>
 * Use JAVA Admin API against cluster
 **/
public class ClusterAdminLocal {

    public static PulsarAdmin createAdmin() {
        System.out.println("Starting createAdmin");
        PulsarAdmin pulsarAdmin = null;
        try {
            pulsarAdmin = PulsarAdmin.builder()
                    //.authentication(getAuth())
                    .serviceHttpUrl(PULSAR_ADMIN_URL)
                    //.tlsTrustCertsFilePath(tlsTrustCertsFilePath)
                    .allowTlsInsecureConnection(false)
                    .build();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
        System.out.println("Returning createAdmin");
        return pulsarAdmin;
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

    /**
     * mvn compile exec:java@cluster_admin
     * <p>
     * uncomment admin code you would like to execute
     **/
    public static void main(String[] args) {

        boolean isDeleteSchema = isDeleteSchema();

        PulsarAdmin pulsarAdmin = createAdmin();

        String topic = Config.StructTopics.ORDER_BACKLOG_CHINA;


        //createTenant(pulsarAdmin, TENANT);
        //createNameSpace(pulsarAdmin, NAME_SPACE);
        //listTopic(pulsarAdmin);
        //listSchema(pulsarAdmin, topic);
        //deleteSchema(isDeleteSchema, pulsarAdmin, topic);


        //deleteTopic(pulsarAdmin, topic);
        //deletePartitionTopic(pulsarAdmin, topic);

        //example of creating partitioned topic

        createPartitionedTopic(pulsarAdmin);


        //example of increasing number of partitions

        //increasePartitions(pulsarAdmin, topic);


        //topics
        //get list of topics and print using forEach loop
        /*
        try{
            List<String> result = pulsarAdmin.topics().getList(namespace);
            result.forEach(string -> System.out.println(string));

        } catch (PulsarAdminException e) {
            e.printStackTrace();;
        }
        */

        //get more information on one particular partition
        //use to see message rate in and messasge rate out for order_approved partition-0, 1 and 2
        /*
        String topicPartition = "persistent://student30/developer/order_approved-partition-0";
        TopicStats stats = null;
        try{
            stats = pulsarAdmin.topics().getStats(topicPartition);
            System.out.println("Stats: " + stats);
            //System.out.println("Messages in: " + stats.getMsgRateIn() + " messages out: " + stats.getMsgRateOut());
        } catch (PulsarAdminException e) {
            e.printStackTrace();
        }
        */

        //get list of subscriptions and their stats, this is a partitioned topic
        /*
        //String topicPartition = "persistent://student30/developer/order_approved-partition-0";
        String topicPartition = "persistent://student30/developer/orderBackLogUS";
        TopicStats stats = null;
        try {
            stats = pulsarAdmin.topics().getStats(topicPartition);
            stats.getSubscriptions().forEach((a,b) -> System.out.println(a + " " + b));
        } catch (PulsarAdminException e) {
            e.printStackTrace();
        }
        */

        //remove subscription
        /*
        String topicGetSubscriptions = "persistent://student01/developer/orderBackLogUS";
        String subscriptionName = "inventory_checker";
        try {
            pulsarAdmin.topics().deleteSubscription(topicGetSubscriptions,subscriptionName);
        } catch (PulsarAdminException e) {
            e.printStackTrace();
        }
        */

        //get backlog quotas before and after making backlog quota
        /*
        try {
            Map<BacklogQuota.BacklogQuotaType,BacklogQuota> quotas = pulsarAdmin.namespaces().getBacklogQuotaMap(namespace);
            quotas.forEach((a,b) -> System.out.println(a + " " + b));  
        } catch (PulsarAdminException e) {
            e.printStackTrace();
        }
        */


        //create backlog quota
        /*
        BacklogQuota myBacklog = null;
        Long limitSize = 100L; //is this number of messages
            //1. orders are stuck in a subscription to orderBackLogChina before we switched to Order Schema
            //2. if redelivery service is not turned on, orders are stuck in subscriptions to order_approved and order_declined
            //3. there could be other places
            //this is a good opportunity to go back and look at active subscriptions on various topics and their stats
            //also deleting unused subscriptions, both of these commands are above
        int limitTime = 300; //300s
        BacklogQuota.RetentionPolicy policy = BacklogQuota.RetentionPolicy.producer_request_hold;
        try {
            myBacklog = new BacklogQuotaImpl(limitSize,limitTime,policy); //not sure why Impl is needed here
            pulsarAdmin.namespaces().setBacklogQuota(namespace, myBacklog);

            //pulsarAdmin.namespaces().setBacklogQuota(namespace, new BacklogQuota(limit, limitTime, policy));
        } catch (PulsarAdminException e) {
            e.printStackTrace();
        }
        */


        //remove backlog quota for namespace
        /*
        try {
            pulsarAdmin.namespaces().removeBacklogQuota(namespace);
        } catch (PulsarAdminException e) {
            e.printStackTrace();
        }
        */

        //get retention policy before and after creating the retention policy
        /*
        try {
            System.out.println(pulsarAdmin.namespaces().getRetention(namespace));
        } catch (PulsarAdminException e) {
            e.printStackTrace();
        }
        */

        //create and set retention policy
        /*
        int retentionTime = 10; // 10 minutes
        int retentionSize = 500; // 500 megabytes
        RetentionPolicies policies = new RetentionPolicies(retentionTime, retentionSize);
        try {
            pulsarAdmin.namespaces().setRetention(namespace, policies);
        } catch (PulsarAdminException e) {
            e.printStackTrace();
        }
        */

        //delete retention policy
        /*
        try {
            pulsarAdmin.namespaces().removeRetention(namespace);
        } catch (PulsarAdminException e) {
            e.printStackTrace();
        }
        */

        //check user account settings at namespace level
        /*
        try {
            System.out.println(pulsarAdmin.namespaces().getPermissions(namespace));
        } catch (PulsarAdminException e) {
            e.printStackTrace();
        }
        */

        //get tenant permissions
        /*
        try {
            System.out.println(pulsarAdmin.tenants().getTenantInfo("student30"));
        } catch (PulsarAdminException e) {
            e.printStackTrace();
        }
        */

        //get list of clusters and their brokers

        /*
        Just with super user permissions, you can get list of clusters and their brokers
        ListClusterAndBrokers(pulsarAdmin);
         */


        
        /* get list of brokers for each partition
        try {
            //get list of partitions returned but brokers are all returning null
            Map<String,String> myMap = pulsarAdmin.lookups().lookupPartitionedTopic("persistent://student01/developer/order_approved");
            myMap.forEach((a,b) -> {
                System.out.println(a + " " + b);
            });
        } catch (PulsarAdminException e) {
            e.printStackTrace();
        }
        */

        //unload topic, watch OrderProducer or InventoryCheckerChina/InventoryCheckerUS when this happens, won't even notice
        //unloadTopic(pulsarAdmin);


        //decrease max publish rate on broker to test producer queue settings
        //limitMaxPublishRateTo1(pulsarAdmin);

        //remove namespace publish rate limitation, used for testing back pressure on producer
        //removeRateLimitation(pulsarAdmin);


        pulsarAdmin.close();
        System.out.println("Exiting");
    }

    private static void createPartitionedTopic(PulsarAdmin pulsarAdmin) {
        String topic;
        int partitions = 3;
        topic = "persistent://public/default/tennis.gamestate";
        try{

            pulsarAdmin.topics().delete(topic);

            System.out.println("Creating partitioned topic");
            pulsarAdmin.topics().createPartitionedTopic(topic, partitions);
            System.out.println("Partitioned topic created");
        } catch (PulsarAdminException e) {
            e.printStackTrace();
        }
    }

    private static void increasePartitions(PulsarAdmin pulsarAdmin, String topic) {
        int partitions = 3;
        try{
            pulsarAdmin.topics().updatePartitionedTopic(topic, partitions);
        } catch (PulsarAdminException e) {
            e.printStackTrace();
        }
    }

    public static void createTenant(PulsarAdmin pulsarAdmin, String tenantName) {

            Set<String> allowedClusters = new HashSet<>();
            allowedClusters.add("cluster-a");

            Set<String> adminRoles = new HashSet<>();
            adminRoles.add("admin");  // Add your admin role(s)


            TenantInfo tenantInfo = TenantInfo.builder()
                    .allowedClusters(allowedClusters)
                    .adminRoles(adminRoles)
                    .build();

        try {
            pulsarAdmin.tenants().createTenant(tenantName, tenantInfo);
        } catch (PulsarAdminException e) {
            e.printStackTrace();
        }
        System.out.println("Tenant created successfully: " + "tenantName");


    }

    private static void createNameSpace(PulsarAdmin pulsarAdmin, String nameSpace) {
        try {
            System.out.println("Creating namespace");
            pulsarAdmin.namespaces().createNamespace(nameSpace);
            System.out.println("Namespace created");
        } catch (PulsarAdminException e) {
            e.printStackTrace();
        }
    }

    private static void removeRateLimitation(PulsarAdmin pulsarAdmin) {
        try {
            System.out.println("Removing rate limitation");
            pulsarAdmin.namespaces().removePublishRate(NAME_SPACE);
            System.out.println("Rate limitation removed");
        } catch (PulsarAdminException e) {
            e.printStackTrace();
        }
    }

    private static void deleteTopic(PulsarAdmin pulsarAdmin, String topic) {
        try{
            pulsarAdmin.topics().delete(topic);
        } catch (PulsarAdminException e) {
            e.printStackTrace();
        }
    }

    private static void deletePartitionTopic(PulsarAdmin pulsarAdmin, String topic) {
        try{
            System.out.println("Deleting partitioned topic");
            pulsarAdmin.topics().deletePartitionedTopic(topic);
            System.out.println("Deleted");
        } catch (PulsarAdminException e) {
            e.printStackTrace();
        }
    }

    private static void unloadTopic(PulsarAdmin pulsarAdmin) {
        try {
            String topicToUnload = "persistent://student30/developer/orderBackLogChina";
            System.out.println("Unloading topic");
            pulsarAdmin.topics().unload(topicToUnload);
        } catch (PulsarAdminException e) {
            e.printStackTrace();
        }
    }

    private static void limitMaxPublishRateTo1(PulsarAdmin pulsarAdmin) {
        try {
            System.out.println("Limiting max publish rate to 1 message per second");
            //1 message per second
            PublishRate msgPubRate = new PublishRate(1,10000);

            pulsarAdmin.namespaces().setPublishRate(NAME_SPACE, msgPubRate);
            System.out.println("Limiting Done");

        } catch (PulsarAdminException e) {
            e.printStackTrace();
        }
    }

    private static void ListClusterAndBrokers(PulsarAdmin pulsarAdmin) {
        try {
            List<String> clusters = pulsarAdmin.clusters().getClusters();
            clusters.forEach(cluster -> {
                try {
                    System.out.println(clusters);
                    List<String> brokers = pulsarAdmin.brokers().getActiveBrokers(cluster);
                    brokers.forEach(broker -> {
                        System.out.println(" " + broker);
                    });
                } catch (PulsarAdminException e) {
                    e.printStackTrace();
                }
            });
        } catch (PulsarAdminException e) {
            e.printStackTrace();
        }
    }

    private static void deleteSchema(boolean isDeleteSchema, PulsarAdmin pulsarAdmin, String topic) {
        if (isDeleteSchema) {
            try {
                pulsarAdmin.schemas().deleteSchema(topic);
                System.out.println("Schema deleted");
            } catch (PulsarAdminException e) {
                e.printStackTrace();
            }
        }
    }

    private static boolean isDeleteSchema() {
        Boolean isDeleteSchema = Boolean.FALSE;
        if (System.getProperty("deleteSchema") != null) {
            isDeleteSchema = (Boolean) System.getProperty("deleteSchema").equals("true");
        }
        System.out.printf("Should deleteSchema: %b%n", isDeleteSchema);
        return isDeleteSchema;
    }

    private static void listSchema( PulsarAdmin pulsarAdmin, String topic) {
        SchemaInfo si = null;
        try {
            si = pulsarAdmin.schemas().getSchemaInfo(topic);
        } catch (PulsarAdminException e) {
            System.out.println("Schema not found");
        }
        System.out.printf("Schema info: %s for topic: %s%n", si, topic);

    }

    private static void listTopic( PulsarAdmin pulsarAdmin) {
        List<String> myTopics = null;
        try {
            myTopics = pulsarAdmin.topics().getList(NAME_SPACE);
        } catch (PulsarAdminException e) {
            e.printStackTrace();
        }
        System.out.printf("Topics in namespace %s:\n", NAME_SPACE);
        myTopics.forEach(System.out::println);
    }

}