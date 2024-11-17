package sn_training.admin_tasks;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2;
import org.apache.pulsar.common.policies.data.*;
import org.apache.pulsar.common.policies.data.impl.BacklogQuotaImpl;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static sn_training.Config.*;
import static sn_training.ConfigLocal.PULSAR_ADMIN_URL;

public class ClusterAdmin {

    public static PulsarAdmin createAdmin() {
        System.out.println("Starting createAdmin");
        PulsarAdmin pulsarAdmin = null;
        try {
            pulsarAdmin = PulsarAdmin.builder()
                    .authentication(getAuth())
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

    public static PulsarAdmin createLocalAdmin() {
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

    public static void main(String[] args) {

        PulsarAdmin pulsarAdmin = createAdmin();
        //PulsarAdmin pulsarAdmin = createLocalAdmin();



        pulsarAdmin.close();
        System.out.println("Exiting");
    }

    private static class Topics {

        private static void listSubscriptionsStats(PulsarAdmin pulsarAdmin, String topic) {

            if(!topic.contains("partition")){
                topic += "-partition-0";
            }
            TopicStats stats = null;
            try {
                stats = pulsarAdmin.topics().getStats(topic);
                stats.getSubscriptions().forEach((a,b) -> System.out.println(a + " " + b));
            } catch (PulsarAdminException e) {
                e.printStackTrace();
            }
        }

        private static void getMessageRateInOut(PulsarAdmin pulsarAdmin, String topic) {
            TopicStats stats = null;
            try{
                stats = pulsarAdmin.topics().getStats(topic);
                System.out.println("Stats: " + stats);
            } catch (PulsarAdminException e) {
                e.printStackTrace();
            }
        }

        private static void listTopic( PulsarAdmin pulsarAdmin, String namespace) {
            List<String> myTopics = null;
            try {
                myTopics = pulsarAdmin.topics().getList(namespace);
                System.out.printf("Topics in namespace %s:\n", namespace);
                myTopics.forEach(System.out::println);

            } catch (PulsarAdminException e) {
                e.printStackTrace();
            }

        }

        private static void createPartitionedTopic(PulsarAdmin pulsarAdmin, String topic, int partitions) {

            try{
                System.out.println("Creating partitioned topic with " + partitions + " partitions");
                pulsarAdmin.topics().createPartitionedTopic(topic, partitions);
                System.out.println("Partitioned topic created" + topic + " with " + partitions + " partitions");
            } catch (PulsarAdminException e) {
                e.printStackTrace();
            }
        }

        private static void unloadTopic(PulsarAdmin pulsarAdmin, String topic) {
            try {
                System.out.println("Unloading topic: " + topic);
                pulsarAdmin.topics().unload(topic);
                System.out.println("Topic: " + topic + "unloaded");
            } catch (PulsarAdminException e) {
                e.printStackTrace();
            }
        }

        private static void deleteTopic(PulsarAdmin pulsarAdmin, String topic) {
            try{
                System.out.println("Deleting topic: " + topic);
                pulsarAdmin.topics().delete(topic);
                System.out.println("Topic: " + topic + " deleted");
            } catch (PulsarAdminException e) {
                e.printStackTrace();
            }
        }

        private static void deletePartitionedTopic(PulsarAdmin pulsarAdmin, String topic) {
            try{
                System.out.println("Deleting partitioned topic: " + topic);
                pulsarAdmin.topics().deletePartitionedTopic(topic);
                System.out.println("Partitioned topic: " + topic + " deleted");
            } catch (PulsarAdminException e) {
                e.printStackTrace();
            }
        }

        private static void increasePartitions(PulsarAdmin pulsarAdmin, String topic) {
            int partitions = 3;
            try{
                System.out.println("Increasing partitions for topic: " + topic);
                pulsarAdmin.topics().updatePartitionedTopic(topic, partitions);
                System.out.println("Partitions increased to " + partitions + " for topic " + topic);
            } catch (PulsarAdminException e) {
                e.printStackTrace();
            }
        }

        private static void listSchema( PulsarAdmin pulsarAdmin, String topic) {

            try {
                System.out.println("Getting schema info for topic: " + topic);
                SchemaInfo schemaInfo = pulsarAdmin.schemas().getSchemaInfo(topic);
                System.out.printf("Schema info: %s for topic: %s%n", schemaInfo, topic);
            } catch (PulsarAdminException e) {
                System.out.println("Schema not found");
            }


        }

        private static void removeSchema(PulsarAdmin pulsarAdmin, String topic) {
            try {
                System.out.println("Deleting schema for topic: " + topic);
                pulsarAdmin.schemas().deleteSchema(topic);
                System.out.println("Schema deleted");
            } catch (PulsarAdminException e) {
                e.printStackTrace();
            }
        }

    }

    private static class Brokers {
        private static void listBrokerPerPartition(PulsarAdmin pulsarAdmin, String topic) {
            try {

                Map<String,String> myMap = pulsarAdmin.lookups().lookupPartitionedTopic(topic);
                myMap.forEach((a,b) -> {
                    System.out.println(a + " " + b);
                });
                if(myMap.isEmpty()){
                    System.out.println("No partitions found for topic " + topic);
                }
            } catch (PulsarAdminException e) {
                e.printStackTrace();
            }
        }
    }

    private static class Subscription {
        private static void remove(PulsarAdmin pulsarAdmin, String topic, String subscriptionName) {
            try {
                pulsarAdmin.topics().deleteSubscription(topic, subscriptionName);
            } catch (PulsarAdminException e) {
                e.printStackTrace();
            }
        }
    }

    private static class Namespace {

        private static void listPermissions(PulsarAdmin pulsarAdmin, String namespace) {
            try {
                Map<String, Set<AuthAction>> permissions = pulsarAdmin.namespaces().getPermissions(namespace);
                System.out.println("Permissions: " + permissions);
            } catch (PulsarAdminException e) {
                e.printStackTrace();
            }
        }

        private static void listTopics(PulsarAdmin pulsarAdmin, String namespace) {
            try{
                List<String> result = pulsarAdmin.topics().getList(namespace);
                result.forEach(System.out::println);
                if(result.isEmpty()){
                    System.out.println("No topics found for namespace " + namespace);
                }

            } catch (PulsarAdminException e) {
                e.printStackTrace();;
            }
        }

        private static void setRetention(PulsarAdmin pulsarAdmin, String namespace) {
            int retentionTime = 10; // 10 minutes
            int retentionSize = 500; // 500 megabytes
            RetentionPolicies policies = new RetentionPolicies(retentionTime, retentionSize);
            System.out.println("Setting retention policy for namespace " + namespace);
            try {
                pulsarAdmin.namespaces().setRetention(namespace, policies);
            } catch (PulsarAdminException e) {
                e.printStackTrace();
            }
        }

        private static void removeRateLimitation(PulsarAdmin pulsarAdmin, String namespace) {
            try {
                System.out.println("Removing rate limitation for namespace: " + namespace);
                pulsarAdmin.namespaces().removePublishRate(namespace);
                System.out.println("Rate limitation removed for namespace: " + namespace);
            } catch (PulsarAdminException e) {
                e.printStackTrace();
            }
        }

        private static void getRetention(PulsarAdmin pulsarAdmin, String namespace) {
            try {
                RetentionPolicies retention = pulsarAdmin.namespaces().getRetention(namespace);
                if(retention == null) {
                    System.out.println("No retention policy set for namespace " + namespace);
                } else {
                    System.out.println("Retention policy for namespace " + namespace + " is " + retention);
                }
            } catch (PulsarAdminException e) {
                e.printStackTrace();
            }
        }

        private static void removeRetentionPolicy(PulsarAdmin pulsarAdmin, String namespace) {
            try {
                System.out.println("Removing retention policy for namespace " + namespace);
                pulsarAdmin.namespaces().removeRetention(namespace);
                System.out.println("Retention policy removed for namespace " + namespace);

                System.out.println("Retention policy removed for namespace " + namespace);
            } catch (PulsarAdminException e) {
                e.printStackTrace();
            }
        }

        private static void listBacklogQuotas(PulsarAdmin pulsarAdmin, String namespace) {
            try {

                Map<BacklogQuota.BacklogQuotaType,BacklogQuota> quotas = pulsarAdmin.namespaces().getBacklogQuotaMap(namespace);

                System.out.println("Backlog quotas for namespace " + namespace + " : ");
                quotas.forEach((a,b) -> System.out.println(a + " " + b));
                if (quotas.isEmpty()) System.out.println("No quotas found for namespace " + namespace);
            } catch (PulsarAdminException e) {
                e.printStackTrace();
            }
        }

        private static void createBacklogQuota(PulsarAdmin pulsarAdmin, String namespace) {

            Long limitSize = 100L;
            int limitTime = 300; //seconds
            BacklogQuota.RetentionPolicy policy = BacklogQuota.RetentionPolicy.producer_request_hold;
            BacklogQuota myBacklog = new BacklogQuotaImpl(limitSize,limitTime,policy);
            try {
                pulsarAdmin.namespaces().setBacklogQuota(namespace, myBacklog);
                System.out.println("Backlog quota set for namespace " + namespace);

                //pulsarAdmin.namespaces().setBacklogQuota(namespace, new BacklogQuota(limit, limitTime, policy));
            } catch (PulsarAdminException e) {
                e.printStackTrace();
            }
        }

        private static void removeBacklogQuota(PulsarAdmin pulsarAdmin, String namespace) {
            try {
                System.out.println("Removing backlog quota for namespace " + namespace);
                pulsarAdmin.namespaces().removeBacklogQuota(namespace);
                System.out.println("Backlog quota removed for namespace " + namespace);
            } catch (PulsarAdminException e) {
                e.printStackTrace();
            }
        }

        private static void limitMaxPublishRateTo1(PulsarAdmin pulsarAdmin, String namespace) {
            try {
                System.out.println("Limiting max publish rate to 1 message per second");
                //1 message per second
                PublishRate msgPubRate = new PublishRate(1,10000);

                pulsarAdmin.namespaces().setPublishRate(namespace, msgPubRate);
                System.out.println("Limiting Done");

            } catch (PulsarAdminException e) {
                e.printStackTrace();
            }
        }


    }

    private static class Tenant {

        private static void getTenantInfo(PulsarAdmin pulsarAdmin, String tenant) {

            try {
                TenantInfo tenantInfo = pulsarAdmin.tenants().getTenantInfo(tenant);
                System.out.println(tenantInfo);
            } catch (PulsarAdminException e) {
                e.printStackTrace();
            }
        }

    }

    private static class Cluster {
        private static void listClusterAndBrokers(PulsarAdmin pulsarAdmin) {
            try {
                List<String> clusters = pulsarAdmin.clusters().getClusters();
                clusters.forEach(cluster -> {
                    try {
                        System.out.println("Cluster: " + cluster + "\n ");
                        List<String> brokers = pulsarAdmin.brokers().getActiveBrokers(cluster);
                        brokers.forEach(broker -> {
                            System.out.println("Broker: " + broker);
                        });
                    } catch (PulsarAdminException e) {
                        e.printStackTrace();
                    }
                });
            } catch (PulsarAdminException e) {
                e.printStackTrace();
            }
        }
    }
}