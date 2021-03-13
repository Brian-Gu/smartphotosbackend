import json
import boto3
import urllib3

from datetime import datetime


def lambda_handler(event, context):

    print(event)

    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    object_name = event["Records"][0]["s3"]["object"]["key"]

    print(bucket_name, object_name)

    rekognition = boto3.client("rekognition", "us-east-1")
    img = {"S3Object": {"Bucket": bucket_name,"Name": object_name}}
    response = rekognition.detect_labels(Image=img, MaxLabels=10, MinConfidence=90)

    labels = [label['Name'] for label in response['Labels']]
    print(labels)

    es_array = json.dumps({
        'objectKey':object_name,
        'bucket':   bucket_name,
        'createdTimestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'labels': labels
    }).encode('utf-8')

    http = urllib3.PoolManager()
    es_search = 'https://search-photos-aghaksxl3t6gsnxniv6snl34oy.us-east-1.es.amazonaws.com/photos/photo'

    headers = urllib3.make_headers(basic_auth='esmaster:Es888888!')
    headers.update({"Content-Type": "application/json"})
    r = http.request('POST', es_search, headers=headers, body=es_array)

    print(es_array)

    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }