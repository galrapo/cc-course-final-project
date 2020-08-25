import os
from pathlib import Path
import boto3
import pydicom
import cv2
import json
import io
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from PIL import Image
import numpy as np
import matplotlib as mpl
from botocore.exceptions import ClientError
from imageio import imread
import base64
# import cStringIO
from io import StringIO

# Define the S3 dest_bucket and object for the medical image we want to analyze.  Also define the color used for redaction.
bucket = 'final-project-medical-images'
object = 'sample-image.jpg'  # can be a path w/ directories
redacted_box_color = 'red'
dpi = 72
phi_detection_threshold = 0.00

valid_list_img_suffix = ['jpg', 'jpeg', 'png', 'tiff', 'dcm']

class ImageAnnonymizer():
    def __init__(self, aws_access_key_id, aws_secret_access_key, aws_session_token=None):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_session_token = aws_session_token
        if aws_session_token is None:
            self.aws_session_token = boto3.Session(profile_name='default')
            # self.client = boto3.client('glue', aws_access_key_id=aws_access_key_id,
            #                            aws_secret_access_key=aws_secret_access_key)
            self.s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                                          aws_secret_access_key=aws_secret_access_key)
            self.iam_client = boto3.client('iam', aws_access_key_id=aws_access_key_id,
                                           aws_secret_access_key=aws_secret_access_key)
            # self.sts_client = boto3.client('sts', aws_access_key_id=aws_access_key_id,
            #                                aws_secret_access_key=aws_secret_access_key)
            # self.log_client = boto3.client('logs', aws_access_key_id=aws_access_key_id,
            #                                aws_secret_access_key=aws_secret_access_key)
            self.rekognition = boto3.client('rekognition', aws_access_key_id=aws_access_key_id,
                                            aws_secret_access_key=aws_secret_access_key)
            self.rekognition = boto3.client(service_name='comprehendmedical',
                                            aws_access_key_id=aws_access_key_id,
                                            aws_secret_access_key=aws_secret_access_key)

        else:
            # self.client = boto3.client('glue', aws_session_token=aws_session_token)
            self.s3_client = boto3.client('s3', aws_session_token=aws_session_token)
            self.iam_client = boto3.client('iam', aws_session_token=aws_session_token)
            # self.sts_client = boto3.client('sts', aws_session_token=aws_session_token)
            # self.log_client = boto3.client('logs', aws_session_token=aws_session_token)
            self.rekognition = boto3.client('rekognition')
            self.rekognition = boto3.client(service_name='comprehendmedical')

        # self.account_id = self.sts_client.get_caller_identity().get('Account')
        # self.logger = CloudWatchLogger(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,
        #                                aws_session_token=aws_session_token,
        #                                log_group='/aws/elastic-anonymization-service/jobs/output')

    def anonymize(self, src_bucket, src_path, s3_bucket_dst, fields: dict, data_format):
        # get image type
        img_type = ImageAnnonymizer.get_image_type(file_name=src_path.split('.')[-1])

        # download image from source bucket
        file_path = self.download_from_s3(src_bucket=src_bucket,
                                          src_key=src_path,
                                          img_type=img_type)

        # convert to png if DICOM type
        if img_type == '.dcm':
            ds = pydicom.read_file(file_path)
            img = ds.pixel_array
            file_path = file_path.replace('.dcm', '.png')
            cv2.imwrite(file_path, img)

        # pre-process
        img = np.array(Image.open(file_path), dtype=np.uint8)
        height, width = img.shape
        figsize = width / float(dpi), height / float(dpi)

        # Use Amazon Rekognition to detect all of the text in the medical image
        # response=rekognition.detect_text(Image={'S3Object':{'Bucket':bucket,'Name':object}})
        response = self.rekognition.detect_text(Image={'Bytes': xray.getvalue()})

        # upload image to target bucket
        pass

    @staticmethod
    def get_image_type(file_name):
        suffix = file_name.split('.')[-1]
        if suffix not in valid_list_img_suffix:
            raise ValueError
        return suffix

    def copy_to_anonymized_bucket(self, src_bucket, src_key, dest_bucket, dest_key):
        copy_source = {
            'Bucket': src_bucket,
            'Key': src_key
        }
        response = self.s3_client.copy_object(Bucket=dest_bucket, CopySource=copy_source, Key=dest_key)
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            print(f"Copy process failed!\nSee details:\n\n{response}")
        else:
            print("Copy successfully accomplished.")

    def download_from_s3(self, src_bucket, src_key, img_type):
        if not os.path.exists('images'):
            os.makedirs('images')
        xray = io.BytesIO() # TODO continue

        tmp_path = "./images/" + src_key.split(f".{img_type}")[0] + "_tmp" + f".{img_type}"
        self.s3_client.download_file(src_bucket, src_key, tmp_path)
        # print(tmp_path)
        return tmp_path

    def upload_to_s3(self, file_name, dest_bucket, dest_file_name=None):
        """Upload a file to an S3 bucket
        :param file_name: File to upload
        :param dest_bucket: Bucket to upload to
        :param dest_file_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """
        # If S3 dest_file_name was not specified, use file_name w/o _tmp suffix
        if dest_file_name is None:
            dest_file_name = file_name.replace("_tmp", '')
        try:
            self.s3_client.upload_file(file_name, dest_bucket, dest_file_name)
        except ClientError as e:
            # self.logger.errore(e)
            # logging.error(e)
            print(f"Error in uploading.\n\n{e}")
            return False
        print("Upload successfully accomplished.")
        return True


def test():
    # glue_wrapper = GlueWrapper(aws_access_key_id="",
    #                            aws_secret_access_key="EhIL7+WB1/W2Kz3h/")
    # glue_wrapper.anonymize(s3_bucket='hw2-data', s3_path="/folder/data", s3_bucket_dst='hw2-data-out',
    #                        fields={"LicensePlate": True, "Sensor": True, "Time": False}, data_format='json')
    IA = ImageAnnonymizer(aws_access_key_id=os.environ['ACCESS_KEY'],
                          aws_secret_access_key=os.environ['SECRET_KEY'])
    # print(f"access key {os.environ['ACCESS_KEY']},   secret key {os.environ['SECRET_KEY']}")
    # IA.copy_to_anonymized_bucket(src_bucket='final-project-medical-images',
    #                              src_key='sample-image.jpg',
    #                              dest_bucket='final-project-data-anonymized',
    #                              dest_key='sample-image-test-cp.jpg')
    # IA.download_from_s3(src_bucket='final-project-medical-images',
    #                     src_key='sample-image.jpg',
    #                     img_type='jpg')
    IA.upload_to_s3(file_name='./images/sample-image_tmp.jpg',
                    dest_bucket='final-project-data-anonymized',
                    dest_file_name='sample-image-test-cp.jpg')


if __name__ == '__main__':
    test()
