package com.letsdata.reader;

import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.AWSSessionCredentialsProvider;
import com.amazonaws.services.kafka.AWSKafka;
import com.amazonaws.services.kafka.AWSKafkaClientBuilder;
import com.amazonaws.services.kafka.model.GetBootstrapBrokersRequest;
import com.amazonaws.services.kafka.model.GetBootstrapBrokersResult;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class KafkaReader {
    private static final Logger logger = LoggerFactory.getLogger(KafkaReader.class);

    private final STSUtil stsUtil;
    private final AWSKafka awsKafka;
    private final KafkaConsumer kafkaConsumer;
    private final String roleArn;
    private final String externalId;
    private final String roleAccessPolicyText;
    private final String roleSessionName;
    private final List<String> managedPolicyArnList;

    public KafkaReader(String region, String clusterArn, String awsAccessKeyId, String awsSecretAccessKey, STSUtil stsUtil, String roleArn, String externalId, String roleAccessPolicyText, String roleSessionName, List<String> managedPolicyArnList) {
        this.stsUtil = stsUtil;
        this.roleArn = roleArn;
        this.externalId = externalId;
        this.roleAccessPolicyText = roleAccessPolicyText;
        this.roleSessionName = roleSessionName;
        this.managedPolicyArnList = managedPolicyArnList;

        this.awsKafka = AWSKafkaClientBuilder.
                standard().
                withRegion(region).
                withCredentials(new AWSSessionCredentialsProvider() {
                    private volatile Credentials credentials;

                    @Override
                    public AWSSessionCredentials getCredentials() {
                        if (credentials == null || credentials.getExpiration().before(new Date(System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(1)))) {
                            refresh();
                        }
                        if (credentials != null && !credentials.getExpiration().before(new Date())) {
                            return new AWSSessionCredentials() {
                                @Override
                                public String getSessionToken() {
                                    return credentials.getSessionToken();
                                }

                                @Override
                                public String getAWSAccessKeyId() {
                                    return credentials.getAccessKeyId();
                                }

                                @Override
                                public String getAWSSecretKey() {
                                    return credentials.getSecretAccessKey();
                                }
                            };
                        } else {
                            throw new RuntimeException("Credentials could not be obtained");
                        }
                    }

                    @Override
                    public void refresh() {
                        AssumeRoleResult stsAssumeRoleResult = stsUtil.assumeRole(roleArn, externalId, roleAccessPolicyText, roleSessionName,  managedPolicyArnList);
                        this.credentials = stsAssumeRoleResult.getCredentials();
                    }
                }).
                build();

        Properties consumerConfig = new Properties();
        try {
            consumerConfig.put("client.id", InetAddress.getLocalHost().getHostName());
            consumerConfig.put("group.id", "foo");
            consumerConfig.put("bootstrap.servers", getBootstrapBrokers(clusterArn));
            consumerConfig.put("security.protocol", "SASL_SSL");
            consumerConfig.put("sasl.mechanism", "AWS_MSK_IAM");
            consumerConfig.put("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
            consumerConfig.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required awsRoleArn=\""+roleArn+"\" awsRoleAccessKeyId=\""+awsAccessKeyId+"\" awsRoleSecretAccessKey=\""+awsSecretAccessKey+"\" awsRoleExternalId=\""+externalId+"\" awsRoleSessionName=\"KafkaConsumer"+UUID.randomUUID().toString()+"\"  awsStsRegion=\""+region+"\";");
            consumerConfig.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consumerConfig.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            logger.debug("creating new consumerClient");
            kafkaConsumer = new KafkaConsumer(consumerConfig);
        } catch (UnknownHostException e) {
            throw new RuntimeException("Unexpected exception in creating kafka consumer", e);
        }
    }

    public String getBootstrapBrokers(String clusterArn) {
        GetBootstrapBrokersRequest getBootstrapBrokersRequest = new GetBootstrapBrokersRequest().withClusterArn(clusterArn);
        GetBootstrapBrokersResult getBootstrapBrokersResult = awsKafka.getBootstrapBrokers(getBootstrapBrokersRequest);
        return getBootstrapBrokersResult.getBootstrapBrokerStringSaslIam();
    }

    public Map<String, List<PartitionInfo>> listTopics() {
        return kafkaConsumer.listTopics();
    }

    public Set<String> listSubscriptions() {
        return kafkaConsumer.subscription();
    }

    public void subscribe(String topicName) {
        kafkaConsumer.subscribe(Arrays.asList(topicName));
    }

    public ConsumerRecords pollTopic(long timeout) {
        return kafkaConsumer.poll(timeout);
    }

    public void commitSync() {
        kafkaConsumer.commitSync();
    }

    public Set<TopicPartition> assignments() {
        return kafkaConsumer.assignment();
    }

    public void assign(String topicName) {
        Set<TopicPartition> topicPartitionSet = new HashSet<>();
        List<PartitionInfo> partitionInfoList = kafkaConsumer.partitionsFor(topicName);
        for(PartitionInfo partitionInfo : partitionInfoList) {
            topicPartitionSet.add(new TopicPartition(topicName, partitionInfo.partition()));
        }
        kafkaConsumer.assign(topicPartitionSet);
        kafkaConsumer.seekToBeginning(topicPartitionSet);
    }

    public Map<String, Map<Integer, Long>> positions() {
        Map<String, Map<Integer, Long>> topicNamePartitionPositionMap = new HashMap<>();
        Map<String, List<PartitionInfo>> topicPartitionInfoMap = listTopics();
        for (String topic : topicPartitionInfoMap.keySet()) {
            for (PartitionInfo topicPartitionInfo : topicPartitionInfoMap.get(topic)){
                String topicName = topicPartitionInfo.topic();
                int partition = topicPartitionInfo.partition();
                TopicPartition topicPartition = new TopicPartition(topicName, partition);
                long position = kafkaConsumer.position(topicPartition);
                if (!topicNamePartitionPositionMap.containsKey(topicName)) {
                    topicNamePartitionPositionMap.put(topicName, new HashMap<>());
                }
                Long existing = topicNamePartitionPositionMap.get(topicName).put(partition, position);
                if (existing != null) {
                    throw new RuntimeException("Unexpected duplicate entry for topic partition");
                }
            }
        }
        return topicNamePartitionPositionMap;
    }
}
