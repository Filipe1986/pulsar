package sn_training;

public class ConfigLocal extends Config {

    public ConfigLocal() {
        super();
    }

    public static String PULSAR_URL = "pulsar://localhost:6650";
    public static String PULSAR_ADMIN_URL = "http://localhost:8080";

    public static final String CREDENTIALS_URL = "file:///Users/goncalvesf2/code/Pulsar/PulsarClassFiles/Developer-Training/src/main/resources/o-mj3r8-student129-10312024.json";
    public static final String ISSUER_URL = "https://auth.streamnative.cloud/";
    public static final String AUDIENCE = "urn:sn:pulsar:o-mj3r8:train";

    public static final String NAME_SPACE = "student129/developer";

    public static final String TOPIC_PREFIX = "persistent://" + NAME_SPACE;

    private static class Suffix {
        public static final String ORDER_BACKLOG_CHINA = "/orderBacklogChina";
        public static final String ORDER_BACKLOG_US = "/orderBacklogUS";
        public static final String ORDER_DECLINED = "/order_declined";
        public static final String ORDER_APPROVED = "/order_approved";
        public static final String SCHEDULED_MARKETING_EMAIL = "/scheduledMarketingEmail";
    }

    public static class Topics {
        public static final String ORDER_BACKLOG_CHINA = TOPIC_PREFIX + Suffix.ORDER_BACKLOG_CHINA;
        public static final String ORDER_BACKLOG_US = TOPIC_PREFIX + Suffix.ORDER_BACKLOG_US;
        public static final String ORDER_APPROVED = TOPIC_PREFIX + Suffix.ORDER_APPROVED;
        public static final String ORDER_DECLINED = TOPIC_PREFIX + Suffix.ORDER_DECLINED;
    }

    public static class StructTopics {

        private static final String STRUCT_SUFFIX = "_struct";
        public static final String ORDER_BACKLOG_CHINA = TOPIC_PREFIX + Suffix.ORDER_BACKLOG_CHINA + STRUCT_SUFFIX;
        public static final String ORDER_BACKLOG_US = TOPIC_PREFIX + Suffix.ORDER_BACKLOG_US + STRUCT_SUFFIX;
        public static final String ORDER_APPROVED = TOPIC_PREFIX + Suffix.ORDER_APPROVED + STRUCT_SUFFIX;
        public static final String ORDER_DECLINED = TOPIC_PREFIX + Suffix.ORDER_DECLINED + STRUCT_SUFFIX;
        public static final String SCHEDULED_MARKETING_EMAIL = TOPIC_PREFIX + Suffix.SCHEDULED_MARKETING_EMAIL + STRUCT_SUFFIX;
    }

    public static class StructDLQTopics {
        private static final String STRUCT_SUFFIX = "_struct";
        private static final String DLQ_SUFFIX = "-DLQ";
        public static final String ORDER_BACKLOG_CHINA = TOPIC_PREFIX + Suffix.ORDER_BACKLOG_CHINA + STRUCT_SUFFIX + DLQ_SUFFIX;
        public static final String ORDER_BACKLOG_US = TOPIC_PREFIX + Suffix.ORDER_BACKLOG_US + STRUCT_SUFFIX + DLQ_SUFFIX;
        public static final String ORDER_APPROVED = TOPIC_PREFIX + Suffix.ORDER_APPROVED + STRUCT_SUFFIX + DLQ_SUFFIX;
        public static final String ORDER_DECLINED = TOPIC_PREFIX + Suffix.ORDER_DECLINED + STRUCT_SUFFIX + DLQ_SUFFIX;
        public static final String SCHEDULED_MARKETING_EMAIL = TOPIC_PREFIX + Suffix.SCHEDULED_MARKETING_EMAIL + STRUCT_SUFFIX + DLQ_SUFFIX;
    }



    public static final String STRING_TOPIC_NAME = TOPIC_PREFIX + "/stringtopic";
    public static final String SETUP_TOPIC_NAME = TOPIC_PREFIX + "/setuptopic";




}
