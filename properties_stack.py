from aws_cdk import (
    Duration, 
    Stack,
    aws_s3 as s3,
    RemovalPolicy as rp,
    aws_dynamodb as dynamodb
)
from constructs import Construct
# defining our custom CDK stack here
class PropertiesStack(Stack):
    # function for initializing the stack
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        # defining our bucket names and identifiers
        processed_bucket_id = 'PROCESSED_BUCKET'
        error_bucket_id = 'ERROR_BUCKET'
        raw_bucket_id = 'RAW_BUCKET'
        raw_bucket_name = 'dct-raw-154wef5'
        table_id = 'PROPERTIES_TABLE'
     # creating the buckets dictionary and importing an existing bucket
        buckets = {
            processed_bucket_id: self.create_bucket(processed_bucket_id)
        }
        buckets[raw_bucket_id] = s3.Bucket.from_bucket_name(self, raw_bucket_id, raw_bucket_name)

        table = self.create_table(table_id)
     # creates an S3 bucket taking the id as the parameter
    def create_bucket(self, id):
        return s3.Bucket(self, id,
            removal_policy=rp.DESTROY
            auto_delete_objects=True
        )
    # creates a dynamoDB table
    def create_table(self, id):
        return dynamodb.Table(self, id,
            partition_key=dynamodb.Attribute(
                name='id',
                type=dynamodb.AttributeType.NUMBER
            ),
            sort_key=dynamodb.Attribute(
                name='creationDate',
                type=dynamodb.AttributeType.STRING
            ),
            removal_policy=rp.DESTROY,
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST
        )


