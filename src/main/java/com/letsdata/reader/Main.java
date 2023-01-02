package com.letsdata.reader;

import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import software.amazon.awssdk.utils.StringUtils;

import java.util.*;

public class Main {

    // $ > letsdatawriteconnector listShards --streamName 'streamName' --customerAccessRoleArn 'customerAccessRoleArn' --awsRegion 'awsRegion' --awsAccessKeyId 'awsAccessKeyId' --awsSecretKey 'awsSecretKey'
    // $ > letsdatawriteconnector getShardIterator --streamName 'streamName' --customerAccessRoleArn 'customerAccessRoleArn' --awsRegion 'awsRegion' --awsAccessKeyId 'awsAccessKeyId' --awsSecretKey 'awsSecretKey' --shardId 'shardId'
    // $ > letsdatawriteconnector getRecords --streamName 'streamName' --customerAccessRoleArn 'customerAccessRoleArn' --awsRegion 'awsRegion' --awsAccessKeyId 'awsAccessKeyId' --awsSecretKey 'awsSecretKey' --shardIterator 'shardIterator'
    public static void main(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("letsdatawriteconnector").build();
        parser.addArgument("action").choices("listShards", "getShardIterator", "getRecords").required(true).help("The kinesis client api method that needs to be called. [\"listShards\", \"getShardIterator\", \"getRecords\"]");
        parser.addArgument("--awsRegion").required(false).type(String.class).help("The awsRegion - default to us-east-1").setDefault("us-east-1");
        parser.addArgument("--awsAccessKeyId").required(true).type(String.class).help("The awsAccessKeyId for the customerAccountForAccess for the dataset");
        parser.addArgument("--customerAccessRoleArn").required(true).type(String.class).help("The customerAccessRoleArn from the dataset that has the been granted the access to the write connector");
        parser.addArgument("--awsSecretKey").required(true).type(String.class).help("The awsSecretKey for the customerAccountForAccess for the dataset");
        parser.addArgument("--streamName").required(true).type(String.class).help("The kinesis stream name");
        parser.addArgument("--shardId").required(false).type(String.class).help("The shardId for the getShardIterator call");
        parser.addArgument("--shardIterator").required(false).type(String.class).help("The shardIterator for the getRecords call");

        try {
            Namespace namespace = parser.parseArgs(args);

            String action = namespace.get("action");
            if (StringUtils.isBlank(action)) {
                throw new ArgumentParserException("action should not be blank", parser);
            }

            String region = namespace.getString("awsRegion");
            String streamName = namespace.getString("streamName");
            String customerAccessRoleArn = namespace.getString("customerAccessRoleArn");
            STSUtil stsUtil = new STSUtil(region, namespace.getString("awsAccessKeyId"), namespace.getString("awsSecretKey"));
            String roleAccessPolicyText = "{\n" +
                    "    \"Version\": \"2012-10-17\",\n" +
                    "    \"Statement\": [\n" +
                    "        {\n" +
                    "            \"Effect\": \"Allow\",\n" +
                    "            \"Action\": [\n" +
                    "                \"kinesis:GetShardIterator\",\n" +
                    "                \"kinesis:GetRecords\"\n" +
                    "            ],\n" +
                    "            \"Resource\": \"arn:aws:kinesis:"+region+":223413462631:stream/"+streamName+"\"\n" +
                    "        },\n" +
                    "        {\n" +
                    "            \"Effect\": \"Allow\",\n" +
                    "            \"Action\": \"kinesis:ListShards\",\n" +
                    "            \"Resource\": \"*\"\n" +
                    "        }\n" +
                    "    ]\n" +
                    "}";

            String roleSessionName = streamName+System.currentTimeMillis();
            KinesisReader kinesisReader = new KinesisReader(region, stsUtil, customerAccessRoleArn, roleAccessPolicyText,roleSessionName, null);
            switch(action) {
                case "listShards": {
                    List<Shard> shardList = kinesisReader.listShards(streamName, null, null);
                    System.out.println(shardList);
                    break;
                }
                case "getShardIterator" : {
                    String shardIterator = kinesisReader.getShardIterator(streamName, namespace.getString("shardId"), ShardIteratorType.TRIM_HORIZON, null, null);
                    System.out.println(shardIterator);
                    break;
                }
                case "getRecords" : {
                    GetRecordsResult getRecordsResult = kinesisReader.getRecords(null, namespace.getString("shardIterator"));
                    System.out.println(getRecordsResult);
                    break;
                }
                default:{
                    throw new ArgumentParserException("Unknown action "+action, parser);
                }
            }
        } catch (ArgumentParserException e) {
            parser.handleError(e);
        }
    }
}
