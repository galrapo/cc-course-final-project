####
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
####


from flask import Flask, render_template, request, redirect, url_for
from botocore.client import Config
import os, json, boto3, logging
import utils
from glue_wrapper import GlueWrapper

app = Flask(__name__)


@app.before_first_request
def init():
    app.logger.addHandler(logging.StreamHandler())
    app.logger.setLevel(logging.INFO)


# Listen for GET requests to yourdomain.com/upload/
@app.route("/")
def account():
    # Show the upload HTML page:
    return render_template('anonymize_request.html')

@app.route("/submit-form/")
def submit_form():
    return render_template('anonymize_request.html')

@app.route("/getcols", methods=["GET"])
def get_cols():
    # R: Pass payload here (bucket config)
    return render_template("column_select.html")

@app.route("/response", methods=["POST"])
def select_cols_form():

    aws_access_key_id = request.form.get('aws-access-key-id')
    aws_access_secret_key = request.form.get('aws-access-secret-key')

    src_bucket = request.form.get('source-s3-bucket')
    src_path = request.form.get('source-path')

    dst_bucket = request.form.get('target-s3-bucket')
    dst_path = request.form.get('target-path')

    schedule = request.form.get('schedule')
    data_type, fields = utils.get_schema(s3_bucket=src_bucket, s3_path=src_path, aws_access_key_id=aws_access_key_id,
                                         aws_secret_access_key=aws_access_secret_key)
    # # collect schema of data stored in s3
    req_data = request.data
    print(req_data)
    # R: can return 200 ok
    return render_template('column_select.html', fileds=fields, data_type=data_type, aws_access_key_id=aws_access_key_id,
                           aws_access_secret_key=aws_access_secret_key, src_bucket=src_bucket, src_path=src_path,
                           target_bucket=dst_bucket, target_path=dst_path, schedule=schedule)
    # return redirect(url_for("getcols"))

@app.route("/anonymize", methods=["POST"])
def start_anonymize():

    aws_access_key_id = request.form.get('aws-access-key-id')
    aws_access_secret_key = request.form.get('aws-access-secret-key')

    src_bucket = request.form.get('source-s3-bucket')
    src_path = request.form.get('source-path')

    dst_bucket = request.form.get('target-s3-bucket')
    dst_path = request.form.get('target-path')
    data_formt = request.form.get('data_format')
    schedule = request.form.get('schedule')
    glue_wrapper = GlueWrapper(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_access_secret_key)
    return None
    glue_wrapper.anonymize(s3_bucket=src_bucket, s3_path=src_path, s3_bucket_dst=dst_bucket, fields=fields, data_format=data_formt, schedule=schedule)
    return render_template('column_select.html')


# Main code
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
