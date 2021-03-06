import json
import boto3
from datetime import datetime, date
import os
region = os.environ['AWS_REGION']

def default(o):
    if isinstance(o, (date, datetime)):
        return o.isoformat()

class CloudWatchLogger(object):

    """
    a logger class that publish the logs to CloudWatch
    """
    def __init__(self, aws_access_key_id, aws_secret_access_key, log_group, aws_session_token=None):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_session_token = aws_session_token
        self.log_group = log_group

        if aws_session_token is None:
            self.log_client = boto3.client('logs', region, aws_access_key_id=aws_access_key_id,
                                           aws_secret_access_key=aws_secret_access_key)
        else:
            self.log_client = boto3.client('logs', region, aws_session_token=aws_session_token)

        self.log_list = []

    def flush(self, base_name):
        """
        Send the logs to CloudWatch. Logs will not show up in CloudWatch until this function is called
        :param base_name: name for the log
        :return:
        """
        try:
            response = self.log_client.describe_log_groups(
                logGroupNamePrefix=self.log_group,
            )

            if len(response['logGroups']) == 0:
                _ = self.log_client.create_log_group(
                    logGroupName=self.log_group
                )

            response = self.log_client.describe_log_streams(
                logGroupName=self.log_group,
                logStreamNamePrefix='log-' + base_name,
            )

            if len(response['logStreams']) == 0:
                _ = self.log_client.create_log_stream(
                    logGroupName=self.log_group,
                    logStreamName='log-' + base_name
                )

            _ = self.log_client.put_log_events(
                logGroupName=self.log_group,
                logStreamName='log-' + base_name,
                logEvents=self.log_list)
            self.log_list = []
        except Exception as e:
            print(e)

    def log(self, message, response_dict=None):

        """
        Add a log entry. a timestamp will be added to each entry.
        :param message:
        :param response_dict: a dictionary containing a boto3 standard response
        :return:
        """
        if response_dict is not None:
            message = message + json.dumps(response_dict, indent=4, default=default)

        timestamp = datetime.now()
        self.log_list.append({
            'timestamp': int(timestamp.timestamp()*1000),
            'message': message
        })
