import json
import os
import boto3
import io
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from PIL import Image
import numpy as np
from botocore.exceptions import ClientError

redacted_box_color = 'black'
dpi = 72
# recall oriented - better safe than sorry
phi_detection_threshold = 0.00
valid_list_img_suffix = ['jpg', 'jpeg', 'png', 'tiff', 'dcm', 'JPG', 'JPEG', 'PNG', 'TIFF', 'DCM']


class ImageAnonymizer():
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
            self.log_client = boto3.client('logs', aws_access_key_id=aws_access_key_id,
                                           aws_secret_access_key=aws_secret_access_key)
            self.rekognition = boto3.client('rekognition', aws_access_key_id=aws_access_key_id,
                                            aws_secret_access_key=aws_secret_access_key)
            self.comprehend_medical = boto3.client(service_name='comprehendmedical',
                                                   aws_access_key_id=aws_access_key_id,
                                                   aws_secret_access_key=aws_secret_access_key)

        else:
            # self.client = boto3.client('glue', aws_session_token=aws_session_token)
            self.s3_client = boto3.client('s3', aws_session_token=aws_session_token)
            self.iam_client = boto3.client('iam', aws_session_token=aws_session_token)
            # self.sts_client = boto3.client('sts', aws_session_token=aws_session_token)
            self.log_client = boto3.client('logs', aws_session_token=aws_session_token)
            self.rekognition = boto3.client('rekognition')
            self.comprehend_medical = boto3.client(service_name='comprehendmedical')

        # self.account_id = self.sts_client.get_caller_identity().get('Account')
        # self.logger = CloudWatchLogger(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,
        #                                aws_session_token=aws_session_token,
        #                                log_group='/aws/elastic-anonymization-service/jobs/output')

    def anonymize_by_path(self, src_bucket, src_path, dest_bucket):
        response = self.s3_client.list_objects_v2(Bucket=src_bucket, Prefix=src_path)
        list_files = [file_content['Key'] for file_content in response['Contents'] \
                      if file_content['Key'].split('.')[-1] in valid_list_img_suffix]
        print(f"Start anonymization of {len(list_files)} images...")
        for file_name in list_files:
            print("-----------------------------------------------------------------------------------")
            print(f"Anonymizing {file_name}")
            self.anonymize_img(src_bucket=src_bucket,
                               src_path=file_name,
                               dest_bucket=dest_bucket)

    def anonymize_img(self, src_bucket, src_path, dest_bucket):
        # get image type
        img_type = ImageAnonymizer.get_image_type(file_name=src_path.split('.')[-1])

        # download image from source bucket
        f = self.download_from_s3(src_bucket=src_bucket,
                                  src_key=src_path,
                                  img_type=img_type)

        # pre-process
        img = np.array(Image.open(f), dtype=np.uint8)

        # Use Amazon Rekognition to detect all of the text in the medical image
        response = self.rekognition.detect_text(Image={'Bytes': f.getvalue()})
        text_detections = response['TextDetections']
        print('Aggregating detected text...')

        text_block = ""
        offset_array = []
        total_length = 0

        # The various text detections are returned in a JSON object.
        # Aggregate the text into a single large block and keep track of the offsets.
        # This will allow us to make a single call to Amazon Comprehend Medical for
        # PHI detection and minimize our Comprehend Medical service charges.
        for text in text_detections:
            if text['Type'] == "LINE":
                offset_array.append(total_length)
                total_length += len(text['DetectedText']) + 1
                text_block = text_block + text['DetectedText'] + " "
                print("adding '" + text['DetectedText'] + "', length: " + str(len(text['DetectedText'])) + \
                      ", offset_array: " + str(offset_array))
        offset_array.append(total_length)
        total_offsets = len(offset_array)

        # Call Amazon Comprehend Medical and pass it the aggregated text from our medical image.
        phi_boxes_list = []
        philist = self.comprehend_medical.detect_phi(Text=text_block)

        # Amazon Comprehend Medical will return a JSON object that contains all of the PHI detected in the text block
        # with offset values that describe where the PHI begins and ends.
        # We can use this to determine which of the text blocks detected by Amazon Rekognition should be redacted.
        # The 'phi_boxes_list' list is created to keep track of the bounding boxes that potentially contain PHI.
        not_redacted = 0
        print('Finding PHI text...')
        for phi in philist['Entities']:
            if phi['Score'] > phi_detection_threshold:
                for i in range(0, total_offsets - 1):
                    if offset_array[i] <= phi['BeginOffset'] < offset_array[i + 1]:
                        if text_detections[i]['Geometry']['BoundingBox'] not in phi_boxes_list:
                            print("'" + phi['Text'] + "' was detected as type '" + \
                                  phi['Type'] + "' and will be redacted.")
                            phi_boxes_list.append(text_detections[i]['Geometry']['BoundingBox'])
            else:
                print("'" + phi['Text'] + "' was detected as type '" + phi['Type'] + \
                      "', but did not meet the confidence score threshold and will not be redacted.")
                not_redacted += 1
        print("Found", len(phi_boxes_list), "text boxes to redact.")
        print(not_redacted, "additional text boxes were detected, but did not meet the confidence score threshold.")

        # Now this list of bounding boxes will be used to draw red boxes over the PHI text.
        if len(img.shape) == 3:
            height, width, channel = img.shape
        else:
            height, width = img.shape
        # What size does the figure need to be in inches to fit the image?
        figsize = width / float(dpi), height / float(dpi)
        # Create a figure of the right size with one axes that takes up the full figure
        fig = plt.figure(figsize=figsize)
        ax = fig.add_axes([0, 0, 1, 1])

        # add anonymization layer on detected areas
        for box in phi_boxes_list:
            # The bounding boxes are described as a ratio of the overall image dimensions, so we must multiply them
            # by the total image dimensions to get the exact pixel values for each dimension.
            x = img.shape[1] * box['Left']
            y = img.shape[0] * box['Top']
            width = img.shape[1] * box['Width']
            height = img.shape[0] * box['Height']
            rect = patches.Rectangle((x, y), width, height,
                                     linewidth=0,
                                     edgecolor=redacted_box_color,
                                     facecolor=redacted_box_color)
            ax.add_patch(rect)

        plt.axis('off')
        plt.gca().xaxis.set_major_locator(plt.NullLocator())
        plt.gca().yaxis.set_major_locator(plt.NullLocator())

        ax.imshow(img)
        plt.imshow(img, cmap='gray')

        buffer = io.BytesIO()
        plt.savefig(buffer, bbox_inches='tight', pad_inches=0, format='png')
        buffer.seek(0)

        # upload redacted medical image, in PNG format to target bucket
        # self.s3_client.put_object(Body=buffer, ContentType='image/png', Key=src_path, Bucket=dest_bucket)
        self.upload_to_s3(img_buffer=buffer,
                          orig_file_name=src_path.split('.')[0],
                          dest_bucket=dest_bucket,
                          img_type=img_type)
        # wrap up
        f.close()
        buffer.close()

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
        # if not os.path.exists('images'):
        #     os.makedirs('images')

        # xray =
        # img_object.download_fileobj(xray)
        # tmp_path = "./images/" + src_key.split(f".{img_type}")[0] + "_tmp" + f".{img_type}"
        f = io.BytesIO()
        self.s3_client.download_fileobj(src_bucket, src_key, f)

        # self.s3_client.download_file(src_bucket, src_key, tmp_path)
        # print(tmp_path)
        # print(np.array(Image.open(f), dtype=np.uint8))
        return f

    def upload_to_s3(self, img_buffer, orig_file_name, dest_bucket, img_type, dest_file_name=None):
        """Upload a img_buffer to an S3 bucket
        :param img_buffer: File to upload
        :param orig_file_name: name of original img_buffer
        :param dest_bucket: Bucket to upload to
        :param img_type: format of image
        :param dest_file_name: S3 object name. If not specified then file_name is used
        :return: True if img_buffer was uploaded, else False
        """
        # If S3 dest_file_name was not specified, use file_name w/o _tmp suffix
        if dest_file_name is None:
            dest_file_name = orig_file_name
        try:
            self.s3_client.put_object(Body=img_buffer,
                                      ContentType=f'image/{img_type}',
                                      Key=f"{dest_file_name}.{img_type}",
                                      Bucket=dest_bucket)
        except ClientError as e:
            # self.logger.error(e)
            # logging.error(e)
            print(f"Error in uploading.\n\n{e}")
            return False
        print(f"{dest_file_name}.{img_type}  Upload successfully accomplished.")
        return True


def test():
    # glue_wrapper = GlueWrapper(aws_access_key_id="",
    #                            aws_secret_access_key="EhIL7+WB1/W2Kz3h/")
    # glue_wrapper.anonymize(s3_bucket='hw2-data', s3_path="/folder/data", dest_bucket='hw2-data-out',
    #                        fields={"LicensePlate": True, "Sensor": True, "Time": False}, data_format='json')
    IA = ImageAnonymizer(aws_access_key_id=os.environ['ACCESS_KEY'],
                         aws_secret_access_key=os.environ['SECRET_KEY'])
    # print(f"access key {os.environ['ACCESS_KEY']},   secret key {os.environ['SECRET_KEY']}")
    # IA.copy_to_anonymized_bucket(src_bucket='final-project-medical-images',
    #                              src_key='sample-image.jpg',
    #                              dest_bucket='final-project-data-anonymized',
    #                              dest_key='sample-image-test-cp.jpg')
    # f = IA.download_from_s3(src_bucket='final-project-medical-images',
    #                         src_key='sample-image.jpg',
    #                         img_type='jpg')
    # IA.upload_to_s3(file_name='./images/sample-image_tmp.jpg',
    #                 dest_bucket='final-project-data-anonymized',
    #                 dest_file_name='sample-image-test-cp.jpg')
    # IA.anonymize_img(src_bucket='final-project-medical-images',
    #                  src_path='sample-image.jpg',
    #                  dest_bucket='final-project-data-anonymized')

    IA.anonymize_by_path(src_bucket='final-project-medical-images',
                         src_path='test/',
                         dest_bucket='final-project-data-anonymized')


def lambda_handler(event, context):
    src_bucket = event['Records'][0]['s3']['bucket']['name']
    src_path = event['Records'][0]['object']['key']
    dest_bucket = os.environ['DEST_PATH']

    IA = ImageAnonymizer(aws_access_key_id=os.environ['ACCESS_KEY'],
                         aws_secret_access_key=os.environ['SECRET_KEY'])

    IA.anonymize_img(src_bucket=src_bucket,
                     src_path=src_path,
                     dest_bucket=dest_bucket)

    return {
        'statusCode': 200,
        'body': json.dumps('Anonymization accomplished successfully!')
    }
