package com.letsdata.reader;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Iterator;

public class KafkaMain {
    // $ > kafka_reader --clusterArn 'clusterArn' --customerAccessRoleArn 'customerAccessRoleArn' --externalId 'externalId' --awsRegion 'awsRegion' --awsAccessKeyId 'awsAccessKeyId' --awsSecretKey 'awsSecretKey' --topicName 'topicName'
    public static void main(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("letsdatawriteconnector").build();
        parser.addArgument("--awsRegion").required(false).type(String.class).help("The awsRegion - default to us-east-1").setDefault("us-east-1");
        parser.addArgument("--awsAccessKeyId").required(true).type(String.class).help("The awsAccessKeyId for the customerAccountForAccess for the dataset");
        parser.addArgument("--customerAccessRoleArn").required(true).type(String.class).help("The customerAccessRoleArn from the dataset that has the been granted the access to the write connector");
        parser.addArgument("--externalId").required(true).type(String.class).help("The externalId for the sts assumeRole. This is the dataset createDatetime.");
        parser.addArgument("--awsSecretKey").required(true).type(String.class).help("The awsSecretKey for the customerAccountForAccess for the dataset");
        parser.addArgument("--clusterArn").required(true).type(String.class).help("The kafka clusterArn");
        parser.addArgument("--topicName").required(true).type(String.class).help("The kafka topic name");

        try {
            Namespace namespace = parser.parseArgs(args);

            String region = namespace.getString("awsRegion");
            String clusterArn = namespace.getString("clusterArn");
            String customerAccessRoleArn = namespace.getString("customerAccessRoleArn");
            String externalId = namespace.getString("externalId");
            STSUtil stsUtil = new STSUtil(region, namespace.getString("awsAccessKeyId"), namespace.getString("awsSecretKey"));
            String roleAccessPolicyText = "{\n" +
                    "    \"Version\": \"2012-10-17\",\n" +
                    "    \"Statement\": [\n" +
                    "        {\n" +
                    "            \"Effect\": \"Allow\",\n" +
                    "            \"Action\": [\n" +
                    "                \"kafka:*\",\n" +
                    "                \"kafka-cluster:*\"\n" +
                    "            ],\n" +
                    "            \"Resource\": \"*\"\n" +
                    "        }\n" +
                    "    ]\n" +
                    "}";
            String action;
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            String roleSessionName = "KafkaReader" + System.currentTimeMillis();
            KafkaReader kafkaReader = new KafkaReader(region, clusterArn, namespace.getString("awsAccessKeyId"), namespace.getString("awsSecretKey"), stsUtil, customerAccessRoleArn, externalId, roleAccessPolicyText, roleSessionName, null);
            do {
                System.out.println("> Enter the kafka consumer method to invoke. [\"listTopics\", \"listSubscriptions\", \"subscribeTopic\", \"pollTopic\", \"commitPolledRecords\", \"topicPartitionPositions\",\"assignTopicPartitions\", \"listAssignments\",\"quit\"]");

                action = reader.readLine();
                switch (action) {
                    case "listTopics": {
                        System.out.println(kafkaReader.listTopics());
                        break;
                    }
                    case "listSubscriptions": {
                        System.out.println(kafkaReader.listSubscriptions());
                        break;
                    }
                    case "subscribeTopic": {
                        kafkaReader.subscribe(namespace.getString("topicName"));
                        break;
                    }
                    case "pollTopic": {
                        ConsumerRecords consumerRecords = kafkaReader.pollTopic(60000);
                        System.out.println("Polled "+consumerRecords.count()+ " records");
                        Iterator<ConsumerRecord> iter = consumerRecords.records(namespace.getString("topicName")).iterator();
                        while (iter.hasNext()) {
                            System.out.println(iter.next().value());
                        }
                        break;
                    }
                    case "commitPolledRecords": {
                        kafkaReader.commitSync();
                        break;
                    }
                    case "topicPartitionPositions": {
                        System.out.println(kafkaReader.positions());
                        break;
                    }
                    case "listAssignments": {
                        System.out.println(kafkaReader.assignments());
                        break;
                    }
                    case "assignTopicPartitions": {
                        kafkaReader.assign(namespace.getString("topicName"));
                        break;
                    }
                    case "quit":{
                        break;
                    }
                    default: {
                        System.out.println("ERROR: Unknown action " + action +", try again");
                    }
                }
            } while (!"quit".equalsIgnoreCase(action));
        } catch (ArgumentParserException e) {
            parser.handleError(e);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
