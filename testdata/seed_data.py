from __future__ import print_function  # Python 2/3 compatibility
import boto3
import time
import csv
import sys


def import_csv(table_name, file_name):
    session = boto3.Session(aws_access_key_id="FAKE", aws_secret_access_key='FAKE_SECRET')
    dynamodb = session.resource('dynamodb', endpoint_url='http://localhost:8000', region_name='us-east-1',)
    dynamodb_table = dynamodb.Table(table_name)
    item_count = 0

    proc_begin_time = time.time()
    with open(file_name, 'r', encoding="utf-8") as csvfile:
        csv_reader = csv.reader(csvfile, delimiter=',')
        for row in csv_reader:
            item_count += 1

            invoice = {'PK': row[0], 'SK': 'root', 'invoiceDate': row[1], 'invoiceBalance': row[2],
                       'invoiceStatus': row[3], 'invoiceDueDate': row[4]}
            dynamodb_table.put_item(Item=invoice)

            invoice_customer = {'PK': row[0], 'SK': row[9]}
            dynamodb_table.put_item(
                Item=invoice_customer)

            bill_invoice = {'PK': row[0], 'SK': row[5], 'billAmount': row[7], 'billBalance': row[8]}
            dynamodb_table.put_item(Item=bill_invoice)

            customer = {'PK': row[9], 'SK': row[0], 'customerName': row[10], 'State': row[11]}
            dynamodb_table.put_item(Item=customer)

            bill = {'PK': row[5], 'SK': row[0], 'billDueDate': row[6], 'billAmount': row[7]}
            dynamodb_table.put_item(Item=bill)

            if item_count % 100 == 0:
                proc_end_time = time.time() - proc_begin_time
                print("Entry count: %s in %s" % (item_count, proc_end_time))
                proc_begin_time = time.time()
        return item_count


if __name__ == "__main__":
    args = sys.argv[1:]
    tableName = args[0]
    fileName = args[1]

    # Capture the execution begin time
    begin_time = time.time()

    # Call the function to Import data into the DynamoDb Table
    count = import_csv(tableName, fileName)

    # Print Execution Summary
    print('RowCount: %s, Total seconds: %s' %
          (count, (time.time() - begin_time)))
