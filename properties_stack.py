from aws_cdk import (
    Duration, 
    Stack,
    aws_s3 as s3,
    aws_s3_notifications as s3_notif,
    RemovalPolicy as rp,
    aws_dynamodb as dynamodb
    aws_lambda
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
        lambda_d = {'id': 'DataValidator', 'path_to_code': '/home/cloudshell-user/properties/src/DataValidator'}
        lambda_s = {'id': 'S3DynamodbUploader', 'path_to_code': '/home/cloudshell-user/properties/src/S3DynamoDBUploader'}

        # creating the buckets dictionary and importing an existing bucket
        buckets = {
            processed_bucket_id: self.create_bucket(processed_bucket_id),
            error_bucket_id: self.create_bucket(error_bucket_id)
        }
        buckets[raw_bucket_id] = s3.Bucket.from_bucket_name(self, raw_bucket_id, raw_bucket_name)
        # Create dynamoDB table
        table = self.create_table(table_id)
        
        # Create Lambda functions
        lambda_funcs = {
            lambda_d['id']: self.create_lambda(lambda_d),
            lambda_s['id']: self.create_lambda(lambda_s)
        }

        # Notifications and permissions
        buckets[raw_bucket_id].add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3_notif.LambdaDestination(lambda_funcs[lambda_d['id']])
        )

        buckets[raw_bucket_id].grant_read(lambda_funcs[lambda_d['id']])

        buckets[error_bucket_id].grant_write(lambda_funcs[lambda_d['id']]) 

        buckets[processed_bucket_id].add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3_notif.LambdaDestination(lambda_funcs[lambda_s['id']])
        )

        buckets[processed_bucket_id].grant_read(lambda_funcs[lambda_s['id']])
        buckets[processed_bucket_id].grant_write(lambda_funcs[lambda_d['id']])

        table.grant_write_data(lambda_funcs[lambda_s['id']])

        # Environmental Variables
        lambda_funcs[lambda_d['id']].add_environment(
            processed_bucket_id, 
            buckets[processed_bucket_id].bucket_name
        )
        lambda_funcs[lambda_d['id']].add_environment(
            error_bucket_id, 
            buckets[error_bucket_id].bucket_name
        )
        lambda_funcs[lambda_s['id']].add_environment(
            table_id, 
            table.table_name
        )

    def create_lambda(self, config):
        return aws_lambda.Function(self, config['id'],
            runtime=aws_lambda.Runtime.PYTHON_3_12,
            timeout=Duration.seconds(10),
            handler='lambda_function.lambda_handler',
            code=aws_lambda.Code.from_asset(config['path_to_code'])
        )

     # creates an S3 bucket taking the id as the parameter
    def create_bucket(self, id):
        return s3.Bucket(self, id,
            removal_policy=rp.DESTROY,
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


