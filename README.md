# letsdata-writeconnector-reader

## About
Sample code to access the write connector data by assuming the customerAccessRole which is granted by #Let's Data. This is needed when write connector resource location is #Let's Data.  See https://www.letsdata.io/docs#customeraccountforaccess (Granting Customer Access to #Let's Data Resources) for details

## Details
The #Let's Data datasets write output records to a write destination and error records to an error destination. These write and error destinations can be:

* either located in the #Let's Data AWS account and be managed by #Let's Data
* or located in the customer's account.

When these error and write destinations are in the customer's account, accessing the output and error records is simple - the customer can use their credentials with the AWS API and access the records.

However, when these error and write destinations are located in the #Let's Data AWS account, the #Let's Data initialization workflow will grant the customer account access to these error and write detinations via an IAM role. This IAM role is listed in the dataset json as the 'customerAccessRoleArn' attribute. We'll also need the dataset's createDatetime which is set as the externalId (contextId) for the sts:assumeRole call:
```
{
    "datasetName": "ExtractTargetUri1222202216",
    "accessGrantRoleArn": "arn:aws:iam::308240606591:role/LetsData_AccessRole_TargetUriExtractor",
    "customerAccountForAccess": "308240606591",

    "customerAccessRoleArn": "arn:aws:iam::223413462631:role/TestCustomerAccess24d29d89b4a2eedc6988cfa17a2c3d81IAMRole",
    "createDatetime": 1685331931671,
    
    "readConnector": {
      "readerType": "SINGLEFILEREADER",
      "bucketName": "commoncrawl",
          ...
    }
}
```    

Here is how this module accesses the data using the customerAccessRoleArn:

* Use AWS SecurityTokenService (AWS STS)'s assumeRole API to get access credentials to the resources. In this case, the customer code is running as the customer's aws account which would then assume the 'customerAccessRoleArn' IAM role. There is one caveat, assumeRole API can assume roles only when running as an IAM user (not as a root account). If the customer code is running as the root account, the assumeRole API will return an error. The simple fix is to create an IAM User and grant it assumeRole access. (We've granted these IAM users AdministratorAccess and that seems to work fine). To follow the AWS security best practices, we've also added an additional externalId (contextId) in the sts:assumeRole call to disallow access in from unknown contexts. Currently, the dataset's createDatetime is set as the externalId.
* Call the write / error destination APIs to get the data using these access credentials. The stream details such as streamName and the error bucketName are in the dataset json.
* A sample implementation of the STS assume role is in the STSUtil.java - this can be used for the Kinesis and S3 destinations. For the Kafka destination, we use the AWS's aws-msk-iam-auth library which uses the same methodology to connect securely to the Kafka cluster. We did make a private fix to this library - you'll need to download our custom version of the jar to access Kafka Cluster. For those interested, issue https://github.com/aws/aws-msk-iam-auth/issues/128 has the details and the fix that we made

Here are details for each of these steps - STS assumeRole API, Kinesis Reader, S3 Reader, IAM User with AdministratorAccess and the cli driver Main class. You can view these code examples in entirety at these github repos. S3 Reader, Kinesis Reader.

### STS Assume Role
* Simple implementation creates an STS client using the IAM User's credentials
* Calls the assumeRole API with the roleArn and policy texts
Implemented in ```STSUtil.java```

### Kinesis Reader
* Create a Kinesis Client using the STS Assume Role utility from the previous step
* Use the Kinesis client to describeStreams, listShards, getShardIterator and getRecords
Implemented in ```KinesisReader.java```

### Kafka Reader
* Create a Kafka Consumer using the `aws-msk-iam-auth` library for auth
* Use the Kafka client to listTopics, assignTopicPartitions, listAssignments, pollTopic, commitPolledRecords, topicPartitionPositions, listSubscriptions, subscribeTopic
  Implemented in ```KafkaReader.java```

### IAM User With AdministratorAccess
The assumeRole API is disallowed for root accounts. The simple fix is to create an IAM User and grant it assumeRole access. (We'll grant these IAM users AdministratorAccess). Then use this user's security credentials in the cli commands.
```
# create an IAM user
$ > aws iam create-user --user-name letsDataReader

# attach user policy to allow AdministratorAccess
$ > aws iam attach-user-policy --policy-arn arn:aws:iam:<ACCOUNT-ID>:aws:policy/AdministratorAccess --user-name letsDataReader
```

## How to Run this Code
* Build the jar by using the following maven commands - this should create a ```letsdata-writeconnector-reader-1.0-SNAPSHOT-jar-with-dependencies.jar``` in the target folder:
```
#download the aws-msk-iam-auth custom jar
$ > curl -o aws-msk-iam-auth-1.1.7-letsdata-custom.jar https://d108vtfcfy7u5c.cloudfront.net/downloads/aws-msk-iam-auth-1.1.7-letsdata-custom.jar

# install the aws-msk-iam-auth-1.1.7-letsdata-custom.jar JAR in the maven repo - update the downloaded path as needed
mvn -e install:install-file -Dfile=aws-msk-iam-auth-1.1.7-letsdata-custom.jar -DgroupId=software.amazon.msk -DartifactId=aws-msk-iam-auth -Dpackaging=jar -Dversion=1.1.7-letsdata-custom

# build the project
$ > cd <github project root>
$ > mvn clean compile assembly:single 
```
### Kinesis
* Run the ```kinesis_reader.sh``` file in the bin folder. You may need to update the jar path as needed. 
* The CLI driver code (```Main.java```) uses the Kinesis Reader and the STS Util from earlier to implement the following CLI commands:
```
# cd into the bin directory
$ > cd src/bin

# awsAccessKeyId and awsSecretKey are the security credentials of an IAM User in the customer AWS account. This is the customer AWS account that was granted access. In case this is a root account, you can create an IAM user. See the "IAM User With AdministratorAccess" section above.

# Given a streamName, list shards for the stream
$ > kinesis_reader.sh listShards --streamName 'streamName' --customerAccessRoleArn 'customerAccessRoleArn' --externalId 'externalId' --awsRegion 'awsRegion' --awsAccessKeyId 'awsAccessKeyId' --awsSecretKey 'awsSecretKey'

# Given a shardId, get the Shard Iterator
$ > kinesis_reader.sh getShardIterator --streamName 'streamName' --customerAccessRoleArn 'customerAccessRoleArn' --externalId 'externalId' --awsRegion 'awsRegion' --awsAccessKeyId 'awsAccessKeyId' --awsSecretKey 'awsSecretKey' --shardId 'shardId'

# Given a shardIterator, get the records from the stream
$ > kinesis_reader.sh getRecords --streamName 'streamName' --customerAccessRoleArn 'customerAccessRoleArn' --awsRegion 'awsRegion' --externalId 'externalId' --awsAccessKeyId 'awsAccessKeyId' --awsSecretKey 'awsSecretKey' --shardIterator 'shardIterator'
```
### Kafka
* Run the ```kafka_reader.sh``` file in the bin folder. You may need to update the jar path as needed.
* The CLI driver code (```KafkaMain.java```) uses the Kafka Reader to implement the following CLI commands:
```
# cd into the bin directory
$ > cd src/bin

# awsAccessKeyId and awsSecretKey are the security credentials of an IAM User in the customer AWS account. This is the customer AWS account that was granted access. In case this is a root account, you can create an IAM user. See the "IAM User With AdministratorAccess" section above.

# Connect a Kafka Consumer to the Kafka Cluster using aws-msk-iam-auth library
$ > kafka_reader --clusterArn 'clusterArn' --customerAccessRoleArn 'customerAccessRoleArn' --externalId 'externalId' --awsRegion 'awsRegion' --awsAccessKeyId 'awsAccessKeyId' --awsSecretKey 'awsSecretKey' --topicName 'topicName'

> Enter the kafka consumer method to invoke. ["listTopics", "listSubscriptions", "subscribeTopic", "pollTopic", "commitPolledRecords", "topicPartitionPositions","assignTopicPartitions", "listAssignments","quit"]
listTopics
{commoncrawl1}

> Enter the kafka consumer method to invoke. ["listTopics", "listSubscriptions", "subscribeTopic", "pollTopic", "commitPolledRecords", "topicPartitionPositions","assignTopicPartitions", "listAssignments","quit"]
assignTopicPartitions

> Enter the kafka consumer method to invoke. ["listTopics", "listSubscriptions", "subscribeTopic", "pollTopic", "commitPolledRecords", "topicPartitionPositions","assignTopicPartitions", "listAssignments","quit"]
topicPartitionPositions
{commoncrawl1={0=0, 1=0, 2=0, 3=0, 4=0}}

> Enter the kafka consumer method to invoke. ["listTopics", "listSubscriptions", "subscribeTopic", "pollTopic", "commitPolledRecords", "topicPartitionPositions","assignTopicPartitions", "listAssignments","quit"]
pollTopic
...
...
...

> Enter the kafka consumer method to invoke. ["listTopics", "listSubscriptions", "subscribeTopic", "pollTopic", "commitPolledRecords", "topicPartitionPositions","assignTopicPartitions", "listAssignments","quit"]
commitPolledRecords

> Enter the kafka consumer method to invoke. ["listTopics", "listSubscriptions", "subscribeTopic", "pollTopic", "commitPolledRecords", "topicPartitionPositions","assignTopicPartitions", "listAssignments","quit"]
topicPartitionPositions
{commoncrawl1={0=179, 1=424, 2=249, 3=185, 4=233}}

> Enter the kafka consumer method to invoke. ["listTopics", "listSubscriptions", "subscribeTopic", "pollTopic", "commitPolledRecords", "topicPartitionPositions","assignTopicPartitions", "listAssignments","quit"]
quit
```
