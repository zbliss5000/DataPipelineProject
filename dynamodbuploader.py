import json
import boto3
import csv
import io
import os

def lambda_handler(event, context):
    s3 = boto3.client('s3')
     
    table_name = os.environ['PROPERTIES_TABLE']
    
    processed_bucket = event['Records'][0]['s3']['bucket']['name']
    csv_filename = event['Records'][0]['s3']['object']['key']
    
    # Download the CSV file from S3, read the content, decode from bytes to string, and split the content by lines
    obj = s3.get_object(Bucket=processed_bucket, Key=csv_filename)
    csv_data = obj['Body'].read().decode('utf-8').splitlines()
    
    required_keys = ['creationDate', 'streetAddress', 'unit', 'bedrooms', 
                    'bathrooms', 'homeType', 'priceChange', 'zipcode', 'city', 
                    'state', 'country', 'livingArea', 'taxAssessedValue', 
                    'priceReduction', 'datePriceChanged', 'homeStatus']

    # Process each row, filter the required keys, and append to the processed rows list
    processed_rows = list()
    reader = csv.DictReader(csv_data)
    for row in reader:
        try:
            property_id = int(row.get('id').strip())
            price = int(row.get('price').strip())
            filtered_row = {'id' : property_id, 'price': price}

            for key in required_keys:
                if key in row:
                    filtered_row[key] = row[key]
            
            processed_rows.append(filtered_row)
        except Exception as e:
            print(f"Error: {e} for row item:\n{row}")

            
    # If the 'processed_rows' list is populated pass it to the 'upload_to_dynamodb' function.
    if processed_rows:
        print(f"Processed data present. Data will be uploaded to '{table_name}'.\n")
        upload_to_dynamodb(table_name, processed_rows)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from S3ToDynamodbUploader!')
    }
    
def upload_to_dynamodb(table_name, items):
    dynamodb = boto3.resource('dynamodb')
    
    table = dynamodb.Table(table_name)
    try:
        with table.batch_writer() as batch: 
            for item in items:
                batch.put_item(Item=item)
    except Exception as e:
        print(f"Error during DynamoDB operation: {e}")