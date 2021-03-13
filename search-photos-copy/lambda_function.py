import math
import dateutil.parser
import datetime
import time
import os
import logging
import boto3
import json
import urllib3


logger = logging.getLogger()
logger.setLevel(logging.ERROR)


# def get_slots(intent_request):
#     return intent_request['currentIntent']['slots']


# def close(session_attributes, fulfillment_state, message):
#     response = {
#         'sessionAttributes': session_attributes,
#         'dialogAction': {
#             'type': 'Close',
#             'fulfillmentState': fulfillment_state,
#             'message': message
#         }
#     }
#     return response


def get_lex_keywords(input):

    lex = boto3.client('lex-runtime', "us-east-1")
    response = lex.post_text(
        botName  = 'SmartPhoto',
        botAlias = 'Prod',
        userId   = 'test',
        inputText = input
    )
    print(response['slots'])

    slots = response['slots']
    keywords = [v for k, v in slots.items() if v]

    return keywords


def lambda_handler(event, context):

    print(event)
    print(context)

    # (1) Get the Keywords from Lex Bot
    # slots = get_slots(event)
    # keywords = [v for k, v in slots.items() if v]

    keywords = get_lex_keywords(event['queryStringParameters']['q'])
    #keywords = get_lex_keywords(event)
    print(keywords)

    # (2) Search the ElasticSearch Engine
    search_kws = ' '.join(keywords)
    search_kws = search_kws.strip()

    es_search = 'https://search-photos-aghaksxl3t6gsnxniv6snl34oy.us-east-1.es.amazonaws.com/photos/_search?q={}'.format(search_kws)

    http = urllib3.PoolManager()
    headers   = urllib3.make_headers(basic_auth='esmaster:Es888888!')
    es_return = http.request('GET', es_search, headers=headers)
    es_return = json.loads(es_return.data.decode('utf-8'))

    object_keys = set()
    try:
        for i in range(len(es_return['hits']['hits'])):
            object_keys.add(es_return['hits']['hits'][i]['_source']['objectKey'])
    except Exception as e:
        pass

    # (3) Return the S3 Locations
    if len(object_keys) == 0:
        content = 'No Photo Found!'
    else:
        content = []
        s3_url = 'https://smartphotos.s3.amazonaws.com/'
        for obj in object_keys:
            content.append(s3_url + '{}'.format(obj))

    print(content)

    # TODO implement
    return {
        'statusCode': 200,
        'headers':{
            'Access-Control-Allow-Origin':'*',
            'Access-Control-Allow-Credentials':True
        },
        'body': json.dumps({"results": content})
    }
