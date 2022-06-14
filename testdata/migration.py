from __future__ import print_function  # Python 2/3 compatibility
import boto3
import sys

def create_table(tableName):
    dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')
    try:
        dynamodb.create_table(
            TableName=tableName,
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
    except:
        print("Table already created")
        pass


if __name__ == "__main__":
    args = sys.argv[1:]
    tableName = args[0]
    create_table(tableName)
