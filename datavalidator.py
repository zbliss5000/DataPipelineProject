import json
import boto3
import csv
import io
import os

def lambda_handler(event, context):
    """Processes an S3 event, separates records into 'processed' and 'error' CSVs, 
    and prepares them for uploading to their respective S3 buckets.
    """
    
    # Initialize the s3 client using boto3.
    s3 = boto3.client('s3')
    
    # Define the name of the processed and error buckets in S3 where you want to copy CSV data into.
    error_bucket = 'ERROR_BUCKET'
    processed_bucket = 'PROCESSED_BUCKET'
    
    # Extract the 'bucket name' and the 'CSV filename' from the 'event' input and print the CSV filename
    raw_bucket = event['Records'][0]['s3']['bucket']['name']
    csv_filename = event['Records'][0]['s3']['object']['key']

    # Download the CSV file from S3, read the content, decode from bytes to string,
    obj = s3.get_object(Bucket=raw_bucket, Key=csv_filename)
    # and split the content by lines
    csv_data = obj['Body'].read().decode('utf-8').splitlines()
    
    # Create two empty lists which will store processed rows and error rows that will be extracted from the CSV data.
    processed_rows = list()
    error_rows = list()
    empty_rows = list()

    # Create a For Loop that reads the CSV content line by line using Python's csv DictReader.
    reader = csv.DictReader(csv_data)
    for row in reader:
        id = row.get('id').strip()
        price = row.get('price').strip()
        home_type = row.get('homeType').strip()
     # Within the loop create an if condition that flags data with a missing price value and appends it to the 'error_rows' list.
     # Else, append it to the processed_rows list.  
        try:
            if price == '':
                raise ValueError("Missing price.")
            elif int(price) <= 0:
                raise ValueError("Negative or zero price.")
            elif home_type.upper() == 'LOT':
                raise ValueError("Invalid LOT home type.")
            else:
                processed_rows.append(row)
        except ValueError as ve:
            error_rows.append(row)
            print(f"Error for record id: {id}. {ve}\nData: {row}\n")
        except Exception as e:
            print(f"Unexpected error: {e} for row item:\n{row}")  

     # If the 'processed_rows' and 'error_rows' lists are populated pass them to the 'upload_to_bucket' function.
    if processed_rows:
        print(f"\nProcessed data present. Data will be uploaded to '{processed_bucket}' at '{csv_filename}'.\n")
    if error_rows:
        print(f"\nError data present. Data will be uploaded to '{error_bucket}' at '{csv_filename}'.\n")


    return {
        'statusCode': 200,
        'body': json.dumps('Hello from DataValidator!')
    }
    
def upload_to_bucket(s3_client, bucket_name, upload_list, csv_filename):
    """Uploads a CSV file created from a list of dictionaries to a specified S3 bucket."""
    field_names = list(upload_list[0].keys())
    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, field_names)
    writer.writeheader()
    writer.writerows(upload_list)
    csv_content = csv_buffer.getvalue()
        
    return s3_client.put_object(Bucket=bucket_name, Key=csv_filename, Body=csv_content)