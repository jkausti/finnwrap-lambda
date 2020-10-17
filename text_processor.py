import boto3
import datetime
import json
import traceback

import finnpos

from subprocess import Popen, PIPE

"""
Lambda function that does the following:

    1. Get content of insert
    2. Parse content
    3. Get data from S3
    4. Insert start time of processing
    5. Call FinnPos with data gotten from S3
    6. Store results in S3
    7. Update dynamoDb record to indicate processing finished

"""


def lambda_handler(event, context):
    print("## EVENT")
    print(event)

    try:
        for record in event['Records']:
            if record['eventName'] == 'INSERT' and record['dynamodb']['NewImage']['sort']['S'].startswith('TEXT_'):
                
                # get path from event
                text_path = record['dynamodb']['NewImage']['preprocessed_text_path']['S']
                username = record['dynamodb']['NewImage']['username']['S']
                bucket_id = record['dynamodb']['NewImage']['bucket_id']['N']
                sort = record['dynamodb']['NewImage']['sort']['S']
                text_id = sort.split('_')[1]
                
                # get bucket
                dyn_db = boto3.client('dynamodb')
                response = dyn_db.get_item(
                    TableName='textapi-dev',
                    Key={
                        'username': {
                            'S': username
                        },
                        'sort': {
                            'S': "BUCKET_{}".format(bucket_id)
                        }
                    }
                )
                bucket_name = response['Item']['bucket_name']['S']

                # fetch text from S3
                s3 = boto3.resource("s3")
                s3_obj = s3.Object(bucket_name, text_path)
                preprocessed_text = s3_obj.get()["Body"].read().decode('utf-8')
                print(preprocessed_text)

                # insert start time of processing
                """
                time = datetime.datetime.now()
                response = dyn_db.update_item(
                    TableName='textapi-dev',
                    Key={
                        'username': {
                            'S': username
                        },
                        'sort': {
                            'S': sort
                        }
                    },
                    UpdateExpression='SET processing_started = {now}'.format(now=time)
                )
                """

                # Process text with finnpos
                args = "./ftb-label"

                process = Popen([args], stdin=PIPE, stdout=PIPE, stderr=PIPE)
                stdout, stderr = process.communicate(input=preprocessed_text.encode())
                analyzed = stdout.decode()
                err = stderr.decode()
                print("## ERRORS")
                print(err)
                
                # process results
                results = finnpos.Finnpos(analyzed)

                # store results in S3
                bucket = s3.Bucket(bucket_name)
                key = "processed_{id}_{username}".format(id=text_id, username=username)
                bucket.put_object(Body=results.getJson(), Key=key)


                response = dyn_db.update_item(
                    TableName='textapi-dev',
                    Key={
                        'username': {
                            'S': username
                        },
                        'sort': {
                            'S': sort
                        }
                    },
                    UpdateExpression='SET #attrName1=:attr_value1, #attrName2=:attr_value2',
                    ExpressionAttributeNames={
                        '#attrName1': 'processed_text_path',
                        '#attrName2': 'processing_complete'
                    },
                    ExpressionAttributeValues={
                        ':attr_value1': {
                            'S': key
                        },
                        ':attr_value2': {
                            'BOOL': True
                        }
                    }
                )
            else:
                print('request not applicable')
    except Exception:
        traceback.print_exc()
