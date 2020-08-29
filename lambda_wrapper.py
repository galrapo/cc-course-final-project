import boto3
import json
from datetime import datetime
import time
import threading
from base_wrapper import BaseWrapper
import os
region = os.environ['AWS_REGION']

class LambdaWrapper(BaseWrapper):

    def __init__(self, aws_access_key_id, aws_secret_access_key, aws_session_token=None):
        super().__init__(aws_access_key_id, aws_secret_access_key, aws_session_token)
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_session_token = aws_session_token

        if aws_session_token is None:
            self.client = boto3.client('lambda', region, aws_access_key_id=aws_access_key_id,
                                       aws_secret_access_key=aws_secret_access_key)
        else:
            self.client = boto3.client('lambda', region, aws_session_token=aws_session_token)

    def anonymize(self, src_bucket, src_path, dst_bucket, continue_sync):
        base_name = datetime.now().strftime("%d%m%Y%H%M%S")
        lambda_name = 'copy-anonymize-' + base_name
        queue_bucket_name = lambda_name + '-queue'

        # create a role for the lambda
        role_arn, role_name = self.create_role(s3_bucket=src_bucket, s3_bucket_dst=dst_bucket, queue_bucket=queue_bucket_name,
                                               base_name=base_name, func_name=lambda_name)
        # wait a few seconds for the role to become active
        time.sleep(10)
        # create lambda function
        func_arn = self.create_lambda(lambda_name=lambda_name, target_bucket=dst_bucket, role_arn=role_arn)

        # create queue and target buckets
        self.create_bucket(bucket_name=queue_bucket_name)
        self.add_rekognition_permission(bucket_name=queue_bucket_name)
    
        self.create_bucket(bucket_name=dst_bucket)

        # add a trigger - each time an item is added to queue - call lambda
        self.add_lambda_trigger(func_arn=func_arn, bucket_name=queue_bucket_name, func_name=lambda_name,
                                statement_id='from-queue')  # to queue bucket

        # if continuous sync, add a trigger - each time an item is added to source bucket - call lambda
        if continue_sync:
            self.add_lambda_trigger(func_arn=func_arn, bucket_name=src_bucket, func_name=lambda_name,
                                    statement_id='from-src')  # to original bucket
            self.add_rekognition_permission(bucket_name=src_bucket)

        # copy all items from source pat to queue
        t = threading.Thread(target=self.copy_to_queue, args=(src_bucket, src_path, queue_bucket_name))
        t.start()
        self._log_flush(base_name)

        return base_name, lambda_name, role_name, queue_bucket_name

    def create_lambda(self, lambda_name, target_bucket, role_arn):

        self._log("Creating lambda: " + lambda_name)
        response = self.client.create_function(
            FunctionName=lambda_name,
            Runtime='python3.8',
            Role=role_arn,
            Handler='image_anonymizer_lambda.lambda_handler',
            Code={
                'S3Bucket': 'anonymization-service-public',
                'S3Key': 'image_anonymizer_lambda.py.zip',
            },
            Description='a function to copy an image from one bucket to another while anonymizing it',
            Timeout=900,
            MemorySize=200,
            Publish=True,
            Environment={
                'Variables': {
                    'DEST_PATH': target_bucket
                }
            },
            Layers=[
                'arn:aws:lambda:us-east-1:770693421928:layer:Klayers-python38-Pillow:4',  # add Pillow layer
            ],
        )
        self._log("create_function response: ", response)

        return response['FunctionArn']

    def create_role(self, s3_bucket, s3_bucket_dst, queue_bucket, base_name, func_name):

        if s3_bucket.startswith('s3://'):
            s3_bucket = s3_bucket[5:]

        role_name = 'AWSLambdaServiceRole-' + base_name
        self._log("Creating role: " + role_name)

        response = self.iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "lambda.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                    }
                ]
            }),
            Path='/service-role/'
        )

        self._log("create_role response: ", response)
        role_arn = response['Role']['Arn']
        response = self.iam_client.create_policy(
            PolicyName=role_name,
            PolicyDocument=json.dumps({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "s3:GetObject",
                            "s3:PutObject",
                            "s3:CreateBucket",
                            "s3:ListObjects"
                        ],
                        "Resource": [
                            "arn:aws:s3:::{}*".format(s3_bucket),
                            "arn:aws:s3:::{}*".format(s3_bucket_dst),
                            "arn:aws:s3:::{}*".format(queue_bucket),
                        ]
                    },
                    {
                        "Effect": "Allow",
                        "Action": "logs:CreateLogGroup",
                        "Resource": "arn:aws:logs:us-east-1:{}:*".format(self.account_id)
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "logs:CreateLogStream",
                            "logs:PutLogEvents"
                        ],
                        "Resource": [
                            "arn:aws:logs:us-east-1:{}:log-group:/aws/lambda/{}:*".format(self.account_id, func_name)
                        ]
                    },
                ]
            })
        )
        self._log("create_policy response: ", response)

        self._log("attaching service policy")
        policy_arn = response['Policy']['Arn']
        response = self.iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn=policy_arn
        )
        self._log("attach_role_policy response: ", response)

        # add AWS Rekognition read only policy
        self._log("attaching AmazonRekognitionReadOnlyAccess policy")
        response = self.iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn='arn:aws:iam::aws:policy/AmazonRekognitionReadOnlyAccess'
        )
        self._log("attach_role_policy response: ", response)

        # add AWS Comprehend Medical Full Access policy
        self._log("attaching ComprehendMedicalFullAccess policy")
        response = self.iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn='arn:aws:iam::aws:policy/ComprehendMedicalFullAccess'
        )
        self._log("attach_role_policy response: ", response)

        return role_arn, role_name

    def add_lambda_trigger(self, func_name, func_arn, bucket_name, statement_id):
        self._log("adding s3 permission to lambda: " + func_name)
        response = self.client.add_permission(
            FunctionName=func_name,
            StatementId=statement_id,
            Action='lambda:InvokeFunction',
            Principal='s3.amazonaws.com',
            SourceArn='arn:aws:s3:::{}'.format(bucket_name),
            SourceAccount=self.account_id
        )
        self._log("add_permission response: ", response)
        self._log("add bucket trigger to bucket: " + bucket_name)
        response = self.s3_client.put_bucket_notification(
            Bucket=bucket_name,
            NotificationConfiguration={
                'CloudFunctionConfiguration': {
                    'Event': 's3:ObjectCreated:*',
                    'CloudFunction': func_arn
                }
            }
        )
        self._log("put_bucket_notification response: ", response)

    def copy_to_queue(self, src_bucket, src_path, dst_bucket):

        self._log("copying images from {}/{} to {}/{}".format(src_bucket, src_path, dst_bucket, src_path))
        s3 = boto3.resource('s3', aws_access_key_id=self.aws_access_key_id,
                            aws_secret_access_key=self.aws_secret_access_key)
        src = s3.Bucket(src_bucket)
        dst = s3.Bucket(dst_bucket)

        for k in src.objects.all():
            if k.key.startswith(src_path):
                dst.copy({'Bucket': src.name, 'Key': k.key}, k.key)

    def add_rekognition_permission(self, bucket_name):
        self._log("add bucket rekogniotion policy: " + bucket_name)
        response = self.s3_client.put_bucket_policy(
            Bucket=bucket_name,
            Policy=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Sid": "AWSRekognitionS3AclBucketRead20191011",
                            "Effect": "Allow",
                            "Principal": {
                                "Service": "rekognition.amazonaws.com"
                            },
                            "Action": [
                                "s3:GetBucketAcl",
                                "s3:GetBucketLocation"
                            ],
                            "Resource": "arn:aws:s3:::{}".format(bucket_name)
                        },
                        {
                            "Sid": "AWSRekognitionS3GetBucket20191011",
                            "Effect": "Allow",
                            "Principal": {
                                "Service": "rekognition.amazonaws.com"
                            },
                            "Action": [
                                "s3:GetObject",
                                "s3:GetObjectAcl",
                                "s3:GetObjectVersion",
                                "s3:GetObjectTagging"
                            ],
                            "Resource": "arn:aws:s3:::{}/*".format(bucket_name)
                        }
                    ]
                }

            )
        )
        self._log("put_bucket_policy response: ", response)
