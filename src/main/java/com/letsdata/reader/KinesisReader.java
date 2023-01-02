package com.letsdata.reader;

import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.AWSSessionCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.*;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.Credentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class KinesisReader {
    private static final Logger logger = LoggerFactory.getLogger(KinesisReader.class);

    private final STSUtil stsUtil;
    private final AmazonKinesis amazonKinesis;
    private final String roleArn;
    private final String roleAccessPolicyText;
    private final String roleSessionName;
    private final List<String> managedPolicyArnList;

    public KinesisReader(String region, STSUtil stsUtil, String roleArn, String roleAccessPolicyText, String roleSessionName, List<String> managedPolicyArnList) {
        this.stsUtil = stsUtil;
        this.roleArn = roleArn;
        this.roleAccessPolicyText = roleAccessPolicyText;
        this.roleSessionName = roleSessionName;
        this.managedPolicyArnList = managedPolicyArnList;

        this.amazonKinesis = AmazonKinesisClientBuilder.
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
                        AssumeRoleResult stsAssumeRoleResult = stsUtil.assumeRole(roleArn, roleAccessPolicyText, roleSessionName,  managedPolicyArnList);
                        this.credentials = stsAssumeRoleResult.getCredentials();
                    }
                }).
                build();
    }

    public DescribeStreamResult describeStream(String streamName, String exclusiveStartShardId) {
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName(streamName);
        if (exclusiveStartShardId != null) {
            describeStreamRequest.setExclusiveStartShardId(exclusiveStartShardId);
        }
        return amazonKinesis.describeStream(describeStreamRequest);
    }

    public List<Shard> listShards(String streamName, Date streamCreationTimestamp, String startToken) {
        List<Shard> shardList = new ArrayList<>();
        String nextToken = startToken;
        do {
            ListShardsRequest listShardsRequest = new ListShardsRequest();
            listShardsRequest.setStreamName(streamName);

            if (streamCreationTimestamp != null) {
                listShardsRequest.setStreamCreationTimestamp(streamCreationTimestamp);
            }

            if (nextToken != null) {
                listShardsRequest.setNextToken(nextToken);
            }
            listShardsRequest.setMaxResults(1000);


            ListShardsResult listShardsResult = null;
            try {
                logger.debug("Executing listShards iteration");
                listShardsResult = amazonKinesis.listShards(listShardsRequest);
                logger.debug("Completed listShards iteration");
            } catch (Exception ex) {
                logger.error(streamName + " listShards threw an exception ", ex.getCause());
                throw new RuntimeException(ex);
            }

            shardList.addAll(listShardsResult.getShards());
            nextToken = listShardsResult.getNextToken();
        } while (nextToken != null);

        return shardList;
    }

    public String getShardIterator(String streamName, String shardId, ShardIteratorType shardIteratorType, String startingSequenceNumber, Date startingTimestamp) {
        GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
        getShardIteratorRequest.setStreamName(streamName);
        getShardIteratorRequest.setShardId(shardId);
        getShardIteratorRequest.setShardIteratorType(shardIteratorType);
        if (startingSequenceNumber != null) {
            getShardIteratorRequest.setStartingSequenceNumber(startingSequenceNumber);
        }

        if (startingTimestamp != null) {
            getShardIteratorRequest.setTimestamp(startingTimestamp);
        }

        GetShardIteratorResult getShardIteratorResult = null;
        try {
            logger.debug("Executing getShardIterator");
            getShardIteratorResult = amazonKinesis.getShardIterator(getShardIteratorRequest);
            logger.debug("Completed getShardIterator");
        } catch (Exception ex) {
            logger.error("shardId " + shardId+ " getShardIterator threw an exception ", ex.getCause());
            throw new RuntimeException(ex);
        }

        return getShardIteratorResult.getShardIterator();
    }

    public GetRecordsResult getRecords(Integer limit, String shardIterator) {
        int recordLimit = limit == null ? 1000 : Math.min(1000, limit);

        GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
        getRecordsRequest.setLimit(recordLimit);
        getRecordsRequest.setShardIterator(shardIterator);

        GetRecordsResult getRecordsResult = null;
        try {
            logger.debug("Executing getRecords");
            getRecordsResult = amazonKinesis.getRecords(getRecordsRequest);
            logger.debug("Completed getRecords");
        } catch (Exception ex) {
            logger.error("shardIterator " + shardIterator + " getRecords threw an exception ", ex.getCause());
            throw new RuntimeException(ex);
        }
        return getRecordsResult;
    }
}
