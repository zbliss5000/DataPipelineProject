import os
# Calling into our csv_processing script from Phase 1
import csv_processing as cp
import boto3

def s3_create_bucket(bucket_name, s3):
    """Create an S3 bucket.

    Parameters
    ----------
    bucket_name : str
        Name of the S3 bucket.
    s3 : boto3.resource
        Boto3 S3 resource.

    Returns
    -------
    dict or None
        The response from the create_bucket call or None if an error occurred.
    """
    # attempts to create the bucket, and catches any exceptions.
    try:
        return s3.create_bucket(Bucket=bucket_name)
    except Exception as e:
        print(f"Error creating bucket: '{bucket_name}'\nDetails: {e}")
        return None
    
    
def s3_upload_files(files, bucket_name, s3):
    """Upload files to an S3 bucket.

    Parameters
    ----------
    files : list of dict
        List of dictionaries containing local file paths and S3 keys.
    bucket_name : str
        Name of the S3 bucket.
    s3 : boto3.resource
        Boto3 S3 resource.
    """
    # iterates through each file_dict in the csv_info_list, and then retrieves the local file path and the corresponding s3 key from the dictionary.
    for file_dict in csv_info_list:
        try:
            local_path, s3_key = file_dict.popitem()
            s3.Bucket(bucket_name).upload_file(local_path, s3_key)
            print(f"The '{local_path}' has been uploaded to '{bucket_name}' at '{s3_key}'.\n")
        except Exception as e:
            print(f"Error uploading files to '{bucket_name}'.\nDetails: {e}")
    
if __name__ == "__main__":
    # Calls the function from csv_processing from Phase 1
    config = cp.get_config()
    print(config)

    if cp.check_directory_exists(config['csv_dir']):
        csv_info_list = cp.id_and_prep_csvs(config)
        

    s3 = boto3.resource('s3')
    # Creates our s3 bucket.
    response = s3_create_bucket(config['bucket_name'], s3)
    print(f"\n{response}\n")
    # Uploads the file into the s3 bucket we created.
    s3_upload_files(csv_info_list, config['bucket_name'], s3)@