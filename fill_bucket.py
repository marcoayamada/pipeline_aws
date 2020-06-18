import boto3
import uuid
import os

def create_bucket_name(bucket_prefix):
    return ''.join([bucket_prefix, str(uuid.uuid4())])


def create_bucket(s3_resource, bucket_prefix):
    try:
        session = boto3.session.Session()
        current_region = session.region_name
        bucket_name = create_bucket_name(bucket_prefix)
        if current_region == 'us-east-1':
            bucket_response = s3_resource.create_bucket(Bucket=bucket_name)
        else:
            bucket_response = s3_resource.create_bucket(
                Bucket=bucket_name,
                CreteBucketConfiguration={'LocationConstraint': current_region})
        print(bucket_name, current_region)
        return bucket_name, bucket_response
    except Exception as e:
        print(e)


def send_files(s3_resource, bucket_name, path):
    try:
        files = os.listdir(path)
        for file in files:
            s3_resource.Bucket(bucket_name).upload_file('{}/{}'.format(path, file), file)
    except Exception as e:
        print(e)


#s3 = boto3.resource('s3')
# # marke90c4416-97b9-4dce-88a0-231054c330f7
#create_bucket(s3, 'mark')
#send_files(s3, 'marke90c4416-97b9-4dce-88a0-231054c330f7', '/home/marco/repositories/pipeline_aws/data')