---
title: Connection Parameters
---
import FunctionDescription from '@site/src/components/FunctionDescription';

<FunctionDescription description="Introduced or updated: v1.2.148"/>

The connection parameters refer to a set of essential connection details required for establishing a secure link to supported external storage services, like Amazon S3. These parameters are enclosed within parentheses and consists of key-value pairs separated by commas. It is commonly utilized in operations such as creating a stage, copying data into Databend, and querying staged files from external sources. The provided key-value pairs offer the necessary authentication and configuration information for the connection.

For example, the following statement creates an external stage on MinIO with the connection parameters:

```sql
CREATE STAGE my_minio_stage URL = 's3://databend' CONNECTION = (ENDPOINT_URL = 'http://localhost:9000', ACCESS_KEY_ID = 'ROOTUSER', SECRET_ACCESS_KEY = 'CHANGEME123', region = 'us-west-2');
```

The connection parameters vary for different storage services based on their specific requirements and authentication mechanisms. For more information, please refer to the tables below.

### Amazon S3-like Storage Services

The following table lists connection parameters for accessing an Amazon S3-like storage service:

| Parameter                 	| Required? 	| Description                                                  	|
|---------------------------	|-----------	|--------------------------------------------------------------	|
| endpoint_url              	| Yes       	| Endpoint URL for Amazon S3-like storage service.             	|
| access_key_id             	| Yes       	| Access key ID for identifying the requester.                 	|
| secret_access_key         	| Yes       	| Secret access key for authentication.                        	|
| allow_anonymous           	| No        	| Whether anonymous access is allowed. Defaults to *false*.    	|
| enable_virtual_host_style 	| No        	| Whether to use virtual host-style URLs. Defaults to *false*. 	|
| master_key                	| No        	| Optional master key for advanced data encryption.            	|
| region                    	| No        	| AWS region where the bucket is located.                      	|
| security_token            	| No        	| Security token for temporary credentials.                    	|

:::note
- If the **endpoint_url** parameter is not specified in the command, Databend will create the stage on Amazon S3 by default. Therefore, when you create an external stage on an S3-compatible object storage or other object storage solutions, be sure to include the **endpoint_url** parameter.

- If you're using S3 storage and your bucket has public read access, you can access and query an external stage associated with the bucket anonymously without providing credentials. To enable this feature, add the **allow_anonymous** parameter to the [storage.s3] section in the *databend-query.toml* configuration file and set it to **true**.
:::

To access your Amazon S3 buckets, you can also specify an AWS IAM role and external ID for authentication. By specifying an AWS IAM role and external ID, you can provide more granular control over which S3 buckets a user can access. This means that if the IAM role has been granted permissions to access only specific S3 buckets, then the user will only be able to access those buckets. An external ID can further enhance security by providing an additional layer of verification. For more information, see https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-role.html

The following table lists connection parameters for accessing Amazon S3 storage service using AWS IAM role authentication:

| Parameter    	| Required? 	| Description                                           	|
|--------------	|-----------	|-------------------------------------------------------	|
| endpoint_url 	| No        	| Endpoint URL for Amazon S3.                           	|
| role_arn     	| Yes       	| ARN of the AWS IAM role for authorization to S3.      	|
| external_id  	| No        	| External ID for enhanced security in role assumption. 	|

### Azure Blob Storage

The following table lists connection parameters for accessing Azure Blob Storage:

| Parameter    	| Required? 	| Description                                         	|
|--------------	|-----------	|-----------------------------------------------------	|
| endpoint_url 	| Yes       	| Endpoint URL for Azure Blob Storage.                	|
| account_key  	| Yes       	| Azure Blob Storage account key for authentication.  	|
| account_name 	| Yes       	| Azure Blob Storage account name for identification. 	|

### Google Cloud Storage

The following table lists connection parameters for accessing Google Cloud Storage:

| Parameter    	| Required? 	| Description                                         	|
|--------------	|-----------	|-----------------------------------------------------	|
| endpoint_url 	| Yes       	| Endpoint URL for Google Cloud Storage.              	|
| credential   	| Yes       	| Google Cloud Storage credential for authentication. 	|

### Alibaba Cloud OSS

The following table lists connection parameters for accessing Alibaba Cloud OSS:

| Parameter            	| Required? 	| Description                                             	|
|----------------------	|-----------	|---------------------------------------------------------	|
| access_key_id        	| Yes       	| Alibaba Cloud OSS access key ID for authentication.     	|
| access_key_secret    	| Yes       	| Alibaba Cloud OSS access key secret for authentication. 	|
| endpoint_url         	| Yes       	| Endpoint URL for Alibaba Cloud OSS.                     	|
| presign_endpoint_url 	| No        	| Endpoint URL for presigning Alibaba Cloud OSS URLs.     	|

### Tencent Cloud Object Storage

The following table lists connection parameters for accessing Tencent Cloud Object Storage (COS):

| Parameter    	| Required? 	| Description                                                 	|
|--------------	|-----------	|-------------------------------------------------------------	|
| endpoint_url 	| Yes       	| Endpoint URL for Tencent Cloud Object Storage.              	|
| secret_id    	| Yes       	| Tencent Cloud Object Storage secret ID for authentication.  	|
| secret_key   	| Yes       	| Tencent Cloud Object Storage secret key for authentication. 	|

### HDFS

The following table lists connection parameters for accessing Hadoop Distributed File System (HDFS):

| Parameter 	| Required? 	| Description                                          	|
|-----------	|-----------	|------------------------------------------------------	|
| name_node 	| Yes       	| HDFS NameNode address for connecting to the cluster. 	|

### WebHDFS

The following table lists connection parameters for accessing WebHDFS:

| Parameter    	| Required? 	| Description                                       	|
|--------------	|-----------	|---------------------------------------------------	|
| endpoint_url 	| Yes       	| Endpoint URL for WebHDFS.                         	|
| delegation   	| No        	| Delegation token for accessing WebHDFS.           	|