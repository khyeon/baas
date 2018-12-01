from __future__ import print_function

import base64
import json
import uuid
from datetime import datetime

import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
from requests_aws4auth import AWS4Auth

# S3 info
BUCKET_NAME = 'eugene.jehmi.com'
FILE_1 = '/tmp/tmp1.json'

# ES info
ES_HOST = 'search-eugene-es-test-3j6tz76jkfc6lijncyu6k2jotm.us-east-1.es.amazonaws.com'  # For example, my-test-domain.us-east-1.es.amazonaws.com
REGION = 'us-east-1'  # e.g. us-west-1


def lambda_handler_transaction(event, context):
    session = boto3.session.Session()
    credentials = session.get_credentials()

    # Get proper credentials for ES auth
    awsauth = AWS4Auth(credentials.access_key,
                       credentials.secret_key,
                       session.region_name, 'es',
                       session_token=credentials.token)

    s3_client = boto3.resource('s3')
    es = Elasticsearch(
        hosts=[{'host': ES_HOST, 'port': 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )

    # s3 info.
    curr_date = datetime.now()
    file_name = curr_date.strftime('%Y%m%d-%H%M%S.%f')
    dir_path = curr_date.strftime('%d/%m/%Y')
    s3_master_name = '{}/{}/{}.json'.format("tx_master", dir_path, file_name)

    # ES info
    index_name = "baas-tx-" + curr_date.strftime('%Y-%m-%d')

    documents = []
    with open(FILE_1, 'w') as file1:
        for record in event['Records']:
            # when occur error, for check kinesis eventID, timestamp
            print("eventID:{},Timestamp:{}".format(record['eventID'], record['kinesis']['approximateArrivalTimestamp']))
            # extract
            encode_data = record['kinesis']['data']
            # decoding, convert to string
            decode_data = base64.b64decode(encode_data).decode("utf-8")

            # json
            decode_json_data = json.loads(decode_data)
            es_json_data = {}
            # file write
            file1.write(decode_data + "\n")

            # every loop initiate
            document = {}
            document["_index"] = index_name
            document["_type"] = "tx"

            # CONFIG transaction is NOT tx_hash
            if decode_json_data["type"] == "CONFIG":
                document["_id"] = str(uuid.uuid4())
            else:
                document["_id"] = decode_json_data["tx_hash"]

            # es source parsing
            es_json_data["transaction_id"] = decode_json_data["transaction_id"]
            es_json_data["block_id"] = decode_json_data["block_id"]
            es_json_data["channel_id"] = decode_json_data["channel_id"]
            es_json_data["channel_name"] = decode_json_data["channel_name"]
            es_json_data["tx_hash"] = decode_json_data["tx_hash"]
            es_json_data["chaincode_name"] = decode_json_data["chaincode_name"]
            es_json_data["function_name"] = decode_json_data["function_name"]
            es_json_data["status"] = decode_json_data["status"]
            es_json_data["creator_msp_id"] = decode_json_data["creator_msp_id"]
            es_json_data["endorser_msp_id"] = decode_json_data["endorser_msp_id"]
            es_json_data["type"] = decode_json_data["type"]
            es_json_data["creator_nonce"] = decode_json_data["creator_nonce"]
            # es_json_data["tx_response"] = decode_json_data["tx_response"] # drop
            es_json_data["to_address"] = decode_json_data["to_address"]
            es_json_data["from_address"] = decode_json_data["from_address"]
            es_json_data["amount"] = decode_json_data["amount"]
            es_json_data["create_dt"] = decode_json_data["create_dt"]
            es_json_data["endorser_status"] = decode_json_data["endorser_status"]
            es_json_data["block_num"] = decode_json_data["block_num"]
            document["_source"] = es_json_data
            documents.append(document)
    print(documents)
    helpers.bulk(es, documents)

    with open(FILE_1, 'rb') as data:
        s3_client.Bucket(BUCKET_NAME).put_object(Key=s3_master_name, Body=data, ACL='public-read')

def lambda_handler_block(event, context):
    session = boto3.session.Session()
    credentials = session.get_credentials()

    # Get proper credentials for ES auth
    awsauth = AWS4Auth(credentials.access_key,
                       credentials.secret_key,
                       session.region_name, 'es',
                       session_token=credentials.token)

    s3_client = boto3.resource('s3')
    es = Elasticsearch(
        hosts=[{'host': ES_HOST, 'port': 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )

    # s3 info.
    curr_date = datetime.now()
    file_name = curr_date.strftime('%Y%m%d-%H%M%S.%f')
    dir_path = curr_date.strftime('%d/%m/%Y')
    s3_master_name = '{}/{}/{}.json'.format("block_master", dir_path, file_name)

    # ES info
    index_name = "baas-block-" + curr_date.strftime('%Y-%m-%d')

    documents = []
    document = {}
    with open(FILE_1, 'w') as file1:
        for record in event['Records']:
            # when occur error, for check kinesis eventID, timestamp
            print("eventID:{},Timestamp:{}".format(record['eventID'], record['kinesis']['approximateArrivalTimestamp']))
            # extract
            encode_data = record['kinesis']['data']
            # decoding, convert to string
            decode_data = base64.b64decode(encode_data).decode("utf-8")
            # json
            decode_json_data = json.loads(decode_data)
            # file write
            file1.write(decode_data + "\n")

            document["_index"] = index_name
            document["_type"] = "block"
            document["_id"] = decode_json_data["block_hash"]
            document["_source"] = decode_json_data
            documents.append(document)

    helpers.bulk(es, documents)

    with open(FILE_1, 'rb') as data:
        s3_client.Bucket(BUCKET_NAME).put_object(Key=s3_master_name, Body=data, ACL='public-read')
