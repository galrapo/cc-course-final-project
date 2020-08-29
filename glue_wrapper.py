import json
import boto3
from datetime import datetime

from base_wrapper import BaseWrapper
import time


def get_code_path():
    return "/" + "/".join(__file__.split('/')[:-1])


class GlueWrapper(BaseWrapper):

    """
    This class handles all anonymization for tabular data.
    It Uses Glue, S3, IAM and STS to detect the data, put it into a table and then remove
    the required information before passing the data to a new bucket.
    """
    def __init__(self, aws_access_key_id, aws_secret_access_key, aws_session_token=None):

        super().__init__(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,
                         aws_session_token=aws_session_token)

        if aws_session_token is None:
            self.client = boto3.client('glue', aws_access_key_id=aws_access_key_id,
                                       aws_secret_access_key=aws_secret_access_key)
        else:
            self.client = boto3.client('glue', aws_session_token=aws_session_token)

    def anonymize(self, s3_bucket, s3_path, s3_bucket_dst, fields: dict, data_format, schedule):

        """
        main entry point.
        orchestrate the entire process
        :param s3_bucket: input bucket (without s3:// prefix)
        :param s3_path: the path for a file or a directory in the bucket
        :param s3_bucket_dst: target bucket, where result data will be saved (without s3:// prefix)
        :param fields: a dictionary of fields defining which data fields should be passed and which
        should be dropped.
        :param data_format: the format of the data (json, csv, parquet)
        :param schedule: defining the schedule for the task (ONCE | DAILY | WEEKLY)
        :return:
        """
        time_signature = datetime.now().strftime("%d%m%Y%H%M%S")

        if not s3_path.startswith('/'):
            s3_path = "/" + s3_path

        schedule_string = ''
        if schedule == 'DAILY':
            schedule_string = 'cron(15 12 * * ? *)'
        elif schedule == 'WEEKLY':
            schedule_string = 'cron(0 0 * * 0)'

        try:
            # create a glue db
            db_name = self.create_db(time_signature)
            # create an IAM role to allow Glue to access the S3 bucket and execute all requests
            role_arn, role_name = self.create_role(db_name=db_name, s3_bucket=s3_bucket, s3_path=s3_path,
                                                   s3_bucket_dst=s3_bucket_dst)
            time.sleep(10)  # wait for the role to become active
            # create a crawler to populate a table with the data extracted from the S3 bucket
            crawler_name = self.create_crawler(db_name=db_name, s3_bucket=s3_bucket,
                                               s3_path=s3_path, role_arn=role_arn, schedule_string=schedule_string)
            self.start_crawler(crawler_name)

            # create target and auxiliary buckets
            self.create_bucket(s3_bucket_dst)
            script_bucket = 'aws-glue-scripts-' + self.account_id
            self.create_bucket(script_bucket)
            table_name = s3_path.split('/')[-1]
            if table_name == '':
                table_name = s3_bucket

            table_name = table_name.replace("-", "_")

            # build the transition script which will drop the defined fields from the data and save it in a new bucket
            script_path = self.upload_transition_script(time_signature, get_code_path() + '/script2.py',
                                                        script_bucket=script_bucket,
                                                        fields=fields, table_name=table_name)
            # create and launch a job using the script
            job_name = self.creat_job(base_name=time_signature, role_arn=role_arn, s3_script_bucket=script_bucket,
                                      script_path=script_path, db_name=db_name, table_name=table_name,
                                      s3_bucket_dst=s3_bucket_dst,
                                      s3_path=s3_path, data_format=data_format)
            self.run_job(job_name=job_name)

            # if needed, schedule the job to run periodically
            if schedule_string != '':
                self.create_trigger(base_name=time_signature, crawler_name=crawler_name, job_name=job_name,
                                    schedule_string=schedule_string)
        except Exception as e:
            self._log('job ended with exception: ' + str(e))
        finally:
            self._log_flush(time_signature)

        return time_signature, self.account_id, s3_bucket_dst

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

    def upload_transition_script(self, base_name, script_path, script_bucket, fields, table_name):

        s3_cript_path = base_name + "-script.py"
        self._log("Uploading script: " + s3_cript_path)

        fields_to_keep = '['
        for field in fields:
            if fields[field]:
                fields_to_keep += '(\"%s\", \"%s\"),' % (field.lower().replace("-", "_"), field.lower().replace("-", "_"))
        fields_to_keep = fields_to_keep[:-1]
        fields_to_keep += ']'

        with open(script_path, 'r') as file:
            data = file.read()
            data = data.replace('[[PLACEHOLDER]]', fields_to_keep)
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
