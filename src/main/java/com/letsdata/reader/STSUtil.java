package com.letsdata.reader;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;
import com.amazonaws.services.securitytoken.model.PolicyDescriptorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class STSUtil {
    private static final Logger logger = LoggerFactory.getLogger(Logger.class);
    private final AWSSecurityTokenService stsClient;

    public STSUtil(String region, String awsAccessKeyId, String awsSecretKey) {
        AWSCredentials awsCredentials = new AWSCredentials() {
            @Override
            public String getAWSAccessKeyId() {
                return awsAccessKeyId;
            }

            @Override
            public String getAWSSecretKey() {
                return awsSecretKey;
            }
        };

        this.stsClient = AWSSecurityTokenServiceClientBuilder.standard().
                withRegion(region).
                withCredentials(new AWSStaticCredentialsProvider(awsCredentials)).
                build();
    }

    public AssumeRoleResult assumeRole(String roleArn, String externalId, String policy, String roleSessionName, List<String> managedPolicyArnList) {
        AssumeRoleRequest request = new AssumeRoleRequest().withRoleArn(roleArn).withPolicy(policy).withRoleSessionName(roleSessionName).withExternalId(externalId);

        if (managedPolicyArnList != null && !managedPolicyArnList.isEmpty()) {
            List<PolicyDescriptorType> policyDescriptorTypeList = new ArrayList<>();
            for (String managedPolicyArn : managedPolicyArnList) {
                policyDescriptorTypeList.add(new PolicyDescriptorType().withArn(managedPolicyArn));
            }
            request.withPolicyArns(policyDescriptorTypeList);
        }

        logger.debug("calling assume role for role arn "+roleArn);

        AssumeRoleResult result = stsClient.assumeRole(request);
        return result;
    }
}
