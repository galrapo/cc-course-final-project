import json
import boto3
from datetime import datetime
from cloud_watch_logger import CloudWatchLogger
import time


def get_code_path():
    return "/" + "/".join(__file__.split('/')[:-1])


class GlueWrapper(object):

    def __init__(self, aws_access_key_id, aws_secret_access_key, aws_session_token=None):

        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_session_token = aws_session_token
        if aws_session_token is None:
            self.client = boto3.client('glue', aws_access_key_id=aws_access_key_id,
                                       aws_secret_access_key=aws_secret_access_key)
            self.s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                                          aws_secret_access_key=aws_secret_access_key)
            self.iam_client = boto3.client('iam', aws_access_key_id=aws_access_key_id,
                                           aws_secret_access_key=aws_secret_access_key)
            self.sts_client = boto3.client('sts', aws_access_key_id=aws_access_key_id,
                                           aws_secret_access_key=aws_secret_access_key)
            self.log_client = boto3.client('logs', aws_access_key_id=aws_access_key_id,
                                           aws_secret_access_key=aws_secret_access_key)
        else:
            self.client = boto3.client('glue', aws_session_token=aws_session_token)
            self.s3_client = boto3.client('s3', aws_session_token=aws_session_token)
            self.iam_client = boto3.client('iam', aws_session_token=aws_session_token)
            self.sts_client = boto3.client('sts', aws_session_token=aws_session_token)
            self.log_client = boto3.client('logs', aws_session_token=aws_session_token)

        self.account_id = self.sts_client.get_caller_identity().get('Account')

        self.logger = CloudWatchLogger(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,
                                       aws_session_token=aws_session_token,
                                       log_group='/aws/elastic-anonymization-service/jobs/output')

    def anonymize(self, s3_bucket, s3_path, s3_bucket_dst, fields: dict, data_format, schedule):

        time_signature = datetime.now().strftime("%d%m%Y%H%M%S")

        schedule_string = ''
        if schedule == 'DAILY':
            schedule_string = 'cron(15 12 * * ? *)'
        elif schedule == 'WEEKLY':
            schedule_string = 'cron(0 0 * * 0)'

        try:
            db_name = self.create_db(time_signature)
            role_arn, role_name = self.create_role(db_name=db_name, s3_bucket=s3_bucket, s3_path=s3_path,
                                                   s3_bucket_dst=s3_bucket_dst)
            time.sleep(10)
            crawler_name = self.create_crawler(db_name=db_name, s3_bucket=s3_bucket,
                                               s3_path=s3_path, role_arn=role_arn, schedule_string=schedule_string)
            self.start_crawler(crawler_name)
            self.create_bucket(s3_bucket_dst)
            script_bucket = 'aws-glue-scripts-' + self.account_id
            self.create_bucket(script_bucket)
            table_name = s3_path.split('/')[-1]
            script_path = self.upload_transition_script(time_signature, get_code_path() + '/script2.py', script_bucket=script_bucket,
                                                        fields=fields, table_name=table_name)
            job_name = self.creat_job(base_name=time_signature, role_arn=role_arn, s3_script_bucket=script_bucket,
                                      script_path=script_path, db_name=db_name, table_name=table_name,
                                      s3_bucket_dst=s3_bucket_dst,
                                      s3_path=s3_path, data_format=data_format)
            self.run_job(job_name=job_name)

            if schedule_string != '':
                self.create_trigger(base_name=time_signature, crawler_name=crawler_name, job_name=job_name,
                                    schedule_string=schedule_string)
        except Exception as e:
            self._log('job ended with exception: ' + str(e))
        finally:
            self._log_flush(time_signature)

        return time_signature, self.account_id, s3_bucket_dst

    def _log_flush(self, base_name):
        self.logger.flush(base_name)

    def _log(self, message, response_dict=None):
        self.logger.log(message=message, response_dict=response_dict)
        print(message)

    def create_db(self, base_name):
        db_name = datetime.now().strftime("database-" + base_name)

        self._log("Creating database: " + db_name)
        response = self.client.create_database(
            DatabaseInput={
                'Name': db_name,
            }
        )
        self._log("create_database response: ", response)
        return db_name

    def create_role(self, db_name, s3_bucket, s3_path, s3_bucket_dst):

        if s3_bucket.startswith('s3://'):
            s3_bucket = s3_bucket[5:]

        role_name = 'AWSGlueServiceRole-' + db_name
        self._log("Creating role: " + role_name)

        response = self.iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps({
              "Version": "2012-10-17",
              "Statement": [
                {
                  "Effect": "Allow",
                  "Principal": {
                    "Service": "glue.amazonaws.com"
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
                            "s3:CreateBucket"
                        ],
                        "Resource": [
                            "arn:aws:s3:::{}{}*".format(s3_bucket, s3_path),
                            "arn:aws:s3:::{}*".format(s3_bucket_dst),
                        ]
                    }
                ]
            })
        )
        self._log("create_policy response: ", response)
        policy_arn = response['Policy']['Arn']
        response = self.iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn=policy_arn
        )
        self._log("attach_role_policy response: ", response)

        response = self.iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn='arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole'
        )
        self._log("attach_role_policy response: ", response)

        return role_arn, role_name

    def create_crawler(self, db_name, s3_bucket, s3_path, role_arn, schedule_string):

        crawler_name = db_name + "-crawler"
        self._log("Creating crawler: " + crawler_name)

        response = self.client.create_crawler(
            Name=crawler_name,
            Role=role_arn,
            DatabaseName=db_name,
            Targets={
                'S3Targets': [
                    {
                        'Path': s3_bucket + s3_path
                    },
                ]
            },
            Schedule=schedule_string,
            SchemaChangePolicy={
                'UpdateBehavior': 'UPDATE_IN_DATABASE',
                'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
            }
        )
        self._log("create_crawler response: ", response)

        return crawler_name

    def creat_job(self, base_name, role_arn, s3_script_bucket, script_path, db_name, table_name, s3_bucket_dst,
                  s3_path, data_format):

        job_name = 'job-' + base_name
        self._log("Creating job: " + job_name)

        response = self.client.create_job(
            Name=job_name,
            Description='copy and anonymize data from {} to {}',
            Role=role_arn,
            ExecutionProperty={
                'MaxConcurrentRuns': 10
            },
            Command={
                'Name': 'glueetl',
                'ScriptLocation': 's3://' + s3_script_bucket + "/" + script_path,
                'PythonVersion': '3'
            },
            DefaultArguments={
                '--JOB_NAME': job_name,
                '--DATABASE_NAME': db_name,
                '--TABLE_NAME': table_name,
                '--BUCKET_NAME': 's3://' + s3_bucket_dst + s3_path,
                '--FORMAT': data_format
            },
            Timeout=2880,
            NumberOfWorkers=10,
            WorkerType='G.1X',
            GlueVersion='2.0'
        )
        self._log("create_job response: ", response)
        return job_name

    def create_bucket(self, bucket_name):

        self._log("Creating bucket: " + bucket_name)
        buckets = self.list_buckets()

        if bucket_name in buckets:
            self._log("Bucket exist: " + bucket_name)
            return True

        try:
            response = self.s3_client.create_bucket(Bucket=bucket_name)
            self._log("create_bucket response: ", response)
        except Exception as e:
            self._log(str(e))
            return False
        return True

    def list_buckets(self):
        response = self.s3_client.list_buckets()

        buckets = []
        for bucket in response['Buckets']:
            buckets.append(bucket["Name"])
        return buckets

    def upload_to_s3(self, body, bucket, key):
        self._log("Uploading to s2: " + bucket + "/" + key)
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

    def upload_transition_script(self, base_name, script_path, script_bucket, fields, table_name):

        s3_cript_path = base_name + "-script.py"
        self._log("Uploading script: " + s3_cript_path)

        fields_to_drop = '['
        for field in fields:
            if not fields[field]:
                fields_to_drop += '\"%s.%s\",' % (table_name, field.lower())
        fields_to_drop = fields_to_drop[:-1]
        fields_to_drop += ']'

        with open(script_path, 'r') as file:
            data = file.read()
            data = data.replace('[[PLACEHOLDER]]', fields_to_drop)
            self.upload_to_s3(data, script_bucket, base_name + "-script.py")
        return s3_cript_path

    def create_table(self, db_name, s3_bucket, s3_path):
        table_name = s3_path.replace('/', '')

        self._log("Creating table: " + table_name)

        response = self.client.create_table(
            DatabaseName=db_name,
            TableInput={
                'Name': table_name,
                'StorageDescriptor': {
                    'Location': 's3://' + s3_bucket + s3_path,
                },
            },
        )
        self._log("create_table response: ", response)

        return table_name

    def run_job(self, job_name):

        self._log("Starting job: " + job_name)
        response = self.client.start_job_run(
            JobName=job_name
        )
        self._log("start_job_run response: ", response)

    def start_crawler(self, crawler_name):
        self._log("Starting crawler: " + crawler_name)

        response = self.client.start_crawler(
            Name=crawler_name
        )
        self._log("start_crawler response: ", response)

    def create_trigger(self, base_name, crawler_name, job_name, schedule_string):

        trigger_name = 'trigger-' + base_name
        self._log("Creating trigger: " + trigger_name)
        response = self.client.create_trigger(
            Name=trigger_name,
            Type='SCHEDULED',
            Schedule=schedule_string,
            Predicate={
                'Logical': 'ANY',
                'Conditions': [
                    {
                        'LogicalOperator': 'EQUALS',
                        'CrawlerName': crawler_name,
                        'CrawlState': 'SUCCEEDED'
                    },
                ]
            },
            Actions=[
                {
                    'JobName': job_name
                },
            ],
        )
        self._log("create_trigger response: ", response)
        return trigger_name

    def create_script(self, ):
        response = self.client.create_script(
            DagNodes=[
                {
                    'Id': 'a',
                    'NodeType': 'ApplyMapping',
                    'Args': [
                        {
                            'Name': 'c',
                            'Value': 'd',
                            'Param': True | False
                        },
                    ],
                },
                {
                    'Id': 'b',
                    'NodeType': 'ApplyMapping',
                    'Args': [
                        {
                            'Name': 'c',
                            'Value': 'd',
                            'Param': True | False
                        },
                    ],
                }
            ],
            DagEdges=[
                {
                    'Source': 'a',
                    'Target': 'b',
                    'TargetParameter': 'g'
                },
            ],
            Language='PYTHON'
        )
        print(response)
