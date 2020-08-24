import json
import boto3
from datetime import datetime, date
import time


def default(o):
    if isinstance(o, (date, datetime)):
        return o.isoformat()


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

        self.log_list = []

    def anonymize(self, s3_bucket, s3_path, s3_bucket_dst, fields: dict, format):

        time_signature = datetime.now().strftime("%d%m%Y%H%M%S")
        db_name = self.create_db(time_signature)
        # s3_path = "/data"
        # s3_bucket = 'hw2-data'
        role_arn, role_name = self.create_role(db_name=db_name, s3_bucket=s3_bucket, s3_path=s3_path)
        time.sleep(10)
        # table_name = self.create_table(db_name=db_name, s3_bucket=s3_bucket, s3_path=s3_path)
        crawler_name = self.create_crawler(db_name=db_name, s3_bucket=s3_bucket,
                                           s3_path=s3_path, role_arn=role_arn)
        self.start_crawler(crawler_name)
        # create output bucket
        # out_bucket = 'hw2-data-out'
        self.create_bucket(s3_bucket_dst)
        # create script bucket
        script_bucket = 'glue-scripts-' + self.account_id
        self.create_bucket(script_bucket)
        table_name = 'tmp'
        script_path = self.upload_transition_script(time_signature, './script2.py', database=db_name, table=table_name,
                                                    dst_bucket=s3_bucket_dst, dst_path=s3_path,
                                                    script_bucket=script_bucket,
                                                    fields=fields, format=format)
        job_name = self.creat_job(base_name=time_signature, role_arn=role_arn, s3_script_bucket=script_bucket,
                                  script_path=script_path, db_name=db_name, table_name=table_name,
                                  s3_bucket_dst=s3_bucket_dst,
                                  s3_path=s3_path, format=format)
        self.run_job(job_name=job_name)

    def _log_flush(self):
        pass
        # response = self.log_client.put_log_events(
        #     logGroupName='string',
        #     logStreamName='string',
        #     logEvents=[
        #         {
        #             'timestamp': 123,
        #             'message': 'string'
        #         },
        #     ],
        #     sequenceToken='string'
        # )

    def _log(self, message, response_dict=None):

        if response_dict is not None:
            message = message + json.dumps(response_dict, indent=4, default=default)

        timestamp = datetime.now()
        self.log_list.append({
                    'timestamp': timestamp,
                    'message': message
                })

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

    def create_role(self, db_name, s3_bucket, s3_path):

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

        self._log("create_role response: " , response)
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
                            "s3:PutObject"
                        ],
                        "Resource": [
                            "arn:aws:s3:::{}{}*".format(s3_bucket, s3_path)
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

    def create_crawler(self, db_name, s3_bucket, s3_path, role_arn):

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
            Schedule='cron(15 12 * * ? *)',
            SchemaChangePolicy={
                'UpdateBehavior': 'UPDATE_IN_DATABASE',
                'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
            }
        )
        self._log("create_crawler response: ", response)

        return crawler_name

    def creat_job(self, base_name, role_arn, s3_script_bucket, script_path, db_name, table_name, s3_bucket_dst,
                  s3_path, format):

        job_name = 'job' + base_name
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
                'JOB_NAME': job_name,
                'DATABASE_NAME': db_name,
                'TABLE_NAME': table_name,
                'BUCKET_NAME': 's3://' + s3_bucket_dst + s3_path,
                'FORMAT': format
            },
            Timeout=288000,
            NumberOfWorkers=10,
            WorkerType='G.1X'
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

    def upload_transition_script(self, base_name, script_path, database, table, dst_bucket, dst_path, script_bucket,
                                 fields, format):

        s3_cript_path = base_name + "-script.py"
        self._log("Uploading script: " + s3_cript_path)
        fields_to_drop = ''
        for field, drop in fields_to_drop:
            if drop:
                fields_to_drop += '\"%s\",' % field.lower

        with open(script_path, 'r') as file:
            data = file.read()
            # data = data.replace('[[database_placeholder]]', database)
            # data = data.replace('[[table_placeholder]]', table)
            # data = data.replace('[[dst_bucket_placeholder]]', dst_bucket)
            # data = data.replace('[[dst_path_placeholder]]', dst_path)
            # data = data.replace('[[format_placeholder]]', format)
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


# glue_wrapper = GlueWrapper(aws_access_key_id="AKIAI3Y2DTRSUEKNVBHQ", aws_secret_access_key="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")
# time_signature = datetime.now().strftime("%d%m%Y%H%M%S")
# script_bucket = 'glue-scripts-' + glue_wrapper.account_id
# glue_wrapper.create_bucket(script_bucket)
# glue_wrapper.upload_transition_script(time_signature, './script.py', database='db', table='table', dst_bucket='dst_bucket', dst_path='data', script_bucket=script_bucket)

glue_wrapper = GlueWrapper(aws_access_key_id="AKIAI3Y2DTRSUEKNVBHQ",
                                   aws_secret_access_key="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")
glue_wrapper.anonymize(s3_bucket='hw2-data', s3_path="/folder/data", s3_bucket_dst='hw2-data-out',
                       fields={"LicensePlate": True, "Sensor": True, "Time": False}, format='json')
# glue_wrapper.upload_transition_script(base_name='24082020131058', script_path='./script2.py', database=, table=, dst_bucket=, dst_path=, script_bucket=, fields=, format=)
# glue_wrapper.creat_job(base_name='24082020131058', script_path=, database, table, dst_bucket, dst_path, script_bucket, fields, format) )
# glue_wrapper.create_crawler(db_name='database-22082020191006', s3_path='s3://hw2-data/data', role_arn='arn:aws:iam::810383843590:role/service-role/AWSGlueServiceRole-database-22082020191006')


