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


# Listen for POST requests to yourdomain.com/submit_form/
# @app.route("/submit-form/", methods=["POST"])
# def submit_form():
#     # Collect the data posted from the HTML form in anonymize_request.html:
#     report = {
#         "aws-access-key-id": request.form["aws-access-key-id"],
#         "aws-access-secret-key": request.form["aws-access-secret-key"],
#         "source-s3-bucket": request.form["source-s3-bucket"],
#         "source-s3-region": request.form["source-s3-region"],
#         "target-s3-bucket": request.form["target-s3-bucket"],
#         "target-s3-region": request.form["target-s3-region"],
#         "report_url": request.form["report-url"]
#     }
#
#     json_payload = json.dumps(report)
#     respond = get_cols(bucket="hw2-data",
#                        prefix="data",
#                        aws_access_key_id="AKIAITTQNPOEPVRUHQUQ",
#                        aws_secret_access_key="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")
#
#     # return render_template('column_select.html')
#     return json_payload, respond

@app.route("/getcols", methods=["GET"])
def get_cols():
    # R: Pass payload here (bucket config)
    return render_template("column_select.html")


@app.route("/submit/", methods=["GET", "POST"])
def select_cols_form():
    print ('x')
    # # collect schema of data stored in s3
    req_data = request.data
    print req_data
    # R: can return 200 ok
    return render_template("column_select.html")
    # return redirect(url_for("getcols"))


# Listen for GET requests to yourdomain.com/sign_s3/
#
# Please see https://gist.github.com/RyanBalfanz/f07d827a4818fda0db81 for an example using
# Python 3 for this view.
@app.route('/sign-s3/')
def sign_s3():
    # Load necessary information into the application
    S3_BUCKET = os.environ.get('S3_BUCKET')
    S3_REGION = os.environ.get('S3_REGION')

    # Load required data from the request
    file_name = request.args.get('file-name')
    file_type = request.args.get('file-type')

    # Initialise the S3 client

    # if you have a CORS configured bucket that is only a few hours old,
    # you may need to use path style addressing for generating pre-signed POSTs and URLs
    # until the necessary DNS changes have time to propagagte.

    s3 = boto3.client('s3',
                      S3_REGION,
                      config=Config(s3={'addressing_style': 'path'}))

    # Generate and return the presigned URL
    presigned_post = s3.generate_presigned_post(
        Bucket=S3_BUCKET,
        Key=file_name,
        Fields={
            "acl": "public-read",
            "Content-Type": file_type},
        Conditions=[
            {"acl": "public-read"},
            {"Content-Type": file_type}
        ],
        ExpiresIn=3600
    )

    # freshly created buckets take time to propagate, use regional endpoint for demo
    # s3_base = s3.amazonaws.com
    s3_base = 's3-%s.amazonaws.com' % S3_REGION

    # Return the data to the client
    return json.dumps({
        'data': presigned_post,
        'url': 'https://%s.%s/%s' % (S3_BUCKET, s3_base, file_name)
    })


# Main code
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
