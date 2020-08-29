import gzip
import json
from json import JSONDecodeError
import boto3
import os

region = os.environ['AWS_REGION']

types = ['csv', 'json', 'gz']
image_types = ['png', 'PNG', 'gif', 'GIF', 'jpg', 'JPG', 'jpeg', 'JPEG']


def get_csv_cols(data):
    return data.splitlines()[0].split(',')


def get_json_cols(data):
    obj = json.loads(data)
    return list(obj[0].keys())


def get_first_key(s3_client, bucket, prefix):
    response = s3_client.list_objects_v2(
        Bucket=bucket, Prefix=prefix,
    )

    if 'Contents' in response:
        for item in response['Contents']:
            key = item['Key']
            if key.find('.') > 0:
                exp = key[key.rfind('.') + 1:]
                if exp in types or exp in image_types:
                    return key
    return None


def get_schema(s3_bucket, s3_path, aws_access_key_id, aws_secret_access_key):
    s3_client = boto3.client('s3', region, aws_access_key_id=aws_access_key_id,
                             aws_secret_access_key=aws_secret_access_key)

    key = get_first_key(s3_client, s3_bucket, s3_path)

    if key is None:
        return None, None

    response = s3_client.get_object(Bucket=s3_bucket, Key=key)

    extension = ''
    if key.find('.') > 0:
        extension = key.split('.')[-1]

    content_type = response['ContentType']
    if content_type.find('image') > 0 or extension in image_types:
        return 'images', None

    if content_type.find('gzip') > 0:
        bytes_data = gzip.decompress(response['Body'].read())
        data = str(bytes_data, 'utf-8')
    else:
        data = response['Body'].read().decode('utf-8')

    if key.find('json') > 0:
        try:
            _ = json.loads(data)
        except JSONDecodeError:
            data = "[" + ",".join(data.splitlines()) + "]"
        fields = get_json_cols(data)
        data_type = 'json'
    elif key.find('csv') > 0:
        fields = get_csv_cols(data)
        data_type = 'csv'
    else:
        raise Exception('file type not supported')

    return data_type, fields
