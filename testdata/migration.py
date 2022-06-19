from __future__ import print_function  # Python 2/3 compatibility
from botocore.exceptions import ClientError
import boto3
import sys


def create_table(table_name):
    session = boto3.Session(aws_access_key_id="FAKE", aws_secret_access_key='FAKE_SECRET')
    dynamodb = session.resource('dynamodb', endpoint_url='http://localhost:8000', region_name='us-east-1',)
    try:
        dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'PK',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'SK',
                    'KeyType': 'RANGE'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'PK',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'SK',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1
            }
        )
    except ClientError:
        print("Table already created")
        pass


if __name__ == "__main__":
    args = sys.argv[1:]
    tableName = args[0]
    create_table(tableName)
