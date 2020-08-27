import boto3
from cloud_watch_logger import CloudWatchLogger


def get_code_path():
    return "/" + "/".join(__file__.split('/')[:-1])


class BaseWrapper(object):

    """
    This class handles all anonymization for tabular data.
    It Uses Glue, S3, IAM and STS to detect the data, put it into a table and then remove
    the required information before passing the data to a new bucket.
    """
    def __init__(self, aws_access_key_id, aws_secret_access_key, aws_session_token=None):

        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_session_token = aws_session_token
        if aws_session_token is None:
            self.s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                                          aws_secret_access_key=aws_secret_access_key)
            self.iam_client = boto3.client('iam', aws_access_key_id=aws_access_key_id,
                                           aws_secret_access_key=aws_secret_access_key)
            self.sts_client = boto3.client('sts', aws_access_key_id=aws_access_key_id,
                                           aws_secret_access_key=aws_secret_access_key)
        else:
            self.s3_client = boto3.client('s3', aws_session_token=aws_session_token)
            self.iam_client = boto3.client('iam', aws_session_token=aws_session_token)
            self.sts_client = boto3.client('sts', aws_session_token=aws_session_token)

        self.account_id = self.sts_client.get_caller_identity().get('Account')

        self.logger = CloudWatchLogger(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,
                                       aws_session_token=aws_session_token,
                                       log_group='/aws/elastic-anonymization-service/jobs/output')

    def _log_flush(self, base_name):
        self.logger.flush(base_name)

    def _log(self, message, response_dict=None):
        self.logger.log(message=message, response_dict=response_dict)
        print(message)

    def create_bucket(self, bucket_name):

        self._log("Creating bucket: " + bucket_name)
        buckets = self.list_buckets()

        if bucket_name in buckets:
            self._log("Bucket exist: " + bucket_name)
            return True

        try:
            response = self.s3_client.create_bucket(Bucket=bucket_name)
            self._log("create_bucket response: ", response)
            bucket_arn = response
        except Exception as e:
            self._log(str(e))
            return False
        return bucket_arn

    def list_buckets(self):
        response = self.s3_client.list_buckets()

        buckets = []
        for bucket in response['Buckets']:
            buckets.append(bucket["Name"])
        return buckets

    def upload_to_s3(self, body, bucket, key):
        self._log("Uploading to s3: " + bucket + "/" + key)
        response = self.s3_client.put_object(
            ACL='private',
            Body=str.encode(body),
            Bucket=bucket,
            ContentEncoding='utf-8',
            ContentType='application/x-python-code',
            Key=key,
            StorageClass='STANDARD'
        )
        self._log("put_object response: ", response)