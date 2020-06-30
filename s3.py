import boto3
import uuid
import logging
import os


def create_bucket_name(bucket_prefix):
    return ''.join([bucket_prefix, str(uuid.uuid4())])


def create_bucket(s3_resource, bucket_prefix):
    session = boto3.session.Session()
    current_region = session.region_name
    bucket_name = create_bucket_name(bucket_prefix)
    if current_region == 'us-east-1':
        bucket_response = s3_resource.create_bucket(Bucket=bucket_name)
    else:
        bucket_response = s3_resource.create_bucket(
            Bucket=bucket_name,
            CreteBucketConfiguration={
                'LocationConstraint': current_region})

    print('>> Bucket name: {}\n>> Region: {}'.format(bucket_name, current_region))
    return bucket_name


def upload_file(s3_resource, path, bucket_name):
    try:
        files = os.listdir(path)
        for file in files:
            full_path_file = '{}/{}'.format(path, file)
            print('Uploading {}....'.format(full_path_file))
            s3_resource.meta.client.upload_file('{}'.format(full_path_file), bucket_name, file)
            print('Upload for {} is done!'.format(file))
    except Exception as e:
        logging.error(e)


def delete_bucket_with_files(s3_resource, bucket_name):
    try:
        bucket = s3_resource.Bucket(bucket_name)
        bucket.objects.all().delete() #delete all files
        bucket.delete() #delete bucket
        return True

    except Exception as e:
        logging.error(e)
        return False


def copy_to_bucket(bucket_from, bucket_to, file_name, resource_s3):
    copy_source = {
        'Bucket': bucket_from,
        'Key': file_name
    }
    resource_s3.meta.client.copy(copy_source, bucket_to, file_name)


def create_temp_file(size, file_name, file_content):
    random_file_name = ''.join([str(uuid.uuid4().hex[:6]), file_name])
    with open(random_file_name, 'w') as f:
        f.write(str(file_content) * size)
    return random_file_name
