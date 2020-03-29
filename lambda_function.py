from datetime import datetime
import json
import logging
import random
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info('Lambda Initialized.')

kinesis = boto3.client('kinesis')

put_count = 300


def lambda_handler(event, context):
    records = []
    for i in range(put_count):
        records.append({
            'Data': json.dumps({'test': 'this is a test record.', 'timestamp': datetime.now().strftime("%Y/%m/%d %H:%M:%S")}),
            'PartitionKey': str(random.randrange(100000))
        })

    response = kinesis.put_records(
        Records=records,
        StreamName='test-stream'
    )

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
