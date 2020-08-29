import os
import boto3
import io
from PIL import Image, ImageDraw
from botocore.exceptions import ClientError
import json

# recall oriented - better safe than sorry
phi_detection_threshold = 0.00
valid_list_img_suffix = ['jpg', 'jpeg', 'png', 'tiff', 'dcm', 'JPG', 'JPEG', 'PNG', 'TIFF', 'DCM']

s3_client = boto3.client('s3')
rekognition_client = boto3.client('rekognition', 'us-east-1')
comprehend_medical = boto3.client(service_name='comprehendmedical')


def anonymize_img(src_bucket, src_path, dest_bucket):
    # get image type
    img_type = get_image_type(file_name=src_path.split('.')[-1])

    # Use Amazon Rekognition to detect all of the text in the medical image
    response = rekognition_client.detect_text(
        Image={
            'S3Object': {
                'Bucket': src_bucket,
                'Name': src_path,
            }
        })

    if 'TextDetections' not in response or len(response['TextDetections']) == 0:
        copy_to_anonymized_bucket(src_bucket=src_bucket, src_key=src_path, dest_bucket=dest_bucket,
                                  dest_key=src_path)
        return

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
            print("adding '" + text['DetectedText'] + "', length: " + str(len(text['DetectedText'])) +
                  ", offset_array: " + str(offset_array))
    offset_array.append(total_length)
    total_offsets = len(offset_array)

    # Call Amazon Comprehend Medical and pass it the aggregated text from our medical image.
    phi_boxes_list = []
    philist = comprehend_medical.detect_phi(Text=text_block)

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
                        print("'" + phi['Text'] + "' was detected as type '" +
                              phi['Type'] + "' and will be redacted.")
                        phi_boxes_list.append(text_detections[i]['Geometry']['BoundingBox'])
        else:
            print("'" + phi['Text'] + "' was detected as type '" + phi['Type'] +
                  "', but did not meet the confidence score threshold and will not be redacted.")
            not_redacted += 1
    print("Found", len(phi_boxes_list), "text boxes to redact.")
    print(not_redacted, "additional text boxes were detected, but did not meet the confidence score threshold.")

    if len(phi_boxes_list) == 0:
        copy_to_anonymized_bucket(src_bucket=src_bucket, src_key=src_path, dest_bucket=dest_bucket,
                                  dest_key=src_path)
    else:

        # download image from source bucket
        in_buffer = download_from_s3(src_bucket=src_bucket, src_key=src_path)

        with Image.open(in_buffer) as img:

            draw = ImageDraw.Draw(img)
            for box in phi_boxes_list:
                x1 = img.size[1] * box['Left']
                y1 = img.size[0] * box['Top']
                x2 = img.size[1] * box['Width'] + x1
                y2 = img.size[0] * box['Height'] + y1
                draw.rectangle([(x1, y1), (x2, y2)], fill='black')

            buffer = io.BytesIO()
            img.save(buffer, img.format)

            upload_to_s3(img_buffer=buffer.getvalue(),
                         orig_file_name=src_path.split('.')[0],
                         dest_bucket=dest_bucket,
                         img_type=img_type)
            in_buffer.close()
            buffer.close()


def get_image_type(file_name):
    suffix = file_name.split('.')[-1]
    if suffix not in valid_list_img_suffix:
        raise ValueError
    return suffix


def copy_to_anonymized_bucket(src_bucket, src_key, dest_bucket, dest_key):
    copy_source = {
        'Bucket': src_bucket,
        'Key': src_key
    }
    response = s3_client.copy_object(Bucket=dest_bucket, CopySource=copy_source, Key=dest_key)
    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        print(f"Copy process failed!\nSee details:\n\n{response}")
    else:
        print("Copy successfully accomplished.")


def download_from_s3(src_bucket, src_key):
    f = io.BytesIO()
    s3_client.download_fileobj(src_bucket, src_key, f)

    return f


def upload_to_s3(img_buffer, orig_file_name, dest_bucket, img_type, dest_file_name=None):
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
        s3_client.put_object(Body=img_buffer,
                             ContentType=f'image/{img_type}',
                             Key=f"{dest_file_name}.{img_type}",
                             Bucket=dest_bucket)
    except ClientError as e:
        print(f"Error in uploading.\n\n{e}")
        return False
    print(f"{dest_file_name}.{img_type}  Upload successfully accomplished.")
    return True


def lambda_handler(event, context):
    src_bucket = event['Records'][0]['s3']['bucket']['name']
    src_path = event['Records'][0]['s3']['object']['key']
    dest_bucket = os.environ['DEST_PATH']

    anonymize_img(src_bucket=src_bucket,
                  src_path=src_path,
                  dest_bucket=dest_bucket)

    return {
        'statusCode': 200,
        'body': json.dumps('Anonymization accomplished successfully!')
    }
