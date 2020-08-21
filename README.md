# AWS Direct Upload Sample

Simple example demonstrating how to accomplish a direct upload to Amazon S3 in a Python Flask application.

Ideally the static assets are processed (minified / compressed) and served from a CDN instead of this app itself.

## Running the application

* Set environment variables for your AWS access key, secret, and bucket name
* Run:

    ```bash
    docker build -t aws-uploads-sample .
    docker run -d --name "uploader" -p 5000:5000 \
        -e "AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}" \
        -e "AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}" \
        -e "S3_BUCKET=${S3_BUCKET}" \
        -e "S3_REGION=${S3_REGION}" \
        aws-uploads-sample
    ```

* Visit [localhost:5000/upload](http://localhost:5000/upload) to try it out

## Troubleshooting

You can test the AWS credentials provided have the rights to upload as follows:

```bash

$ docker exec -it uploader sh

$ python
import boto3
from botocore.client import Config
s3 = boto3.client('s3', 'ap-southeast-1',config=Config(s3={'addressing_style':'path'}))
s3.upload_file('application.py','myBucket','application.py')
```

## Source

This sample app is a direct copy of:

- [FlaskDirectUploader](https://github.com/flyingsparx/FlaskDirectUploader)
- [Direct to S3 File Uploads in Python](https://devcenter.heroku.com/articles/s3-upload-python)

## Licensing

The files in this repository are, unless stated otherwise, released under the Apache License. You are free to redistribute this code with or without modification. The full license text is available [here](http://www.apache.org/licenses/LICENSE-2.0).

