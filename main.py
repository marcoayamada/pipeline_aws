import time
import os
import boto3
import configparser
from iam import set_role_policy
from s3 import create_bucket, upload_file, delete_bucket_with_files
from redshift import create_redshift_cluster, delete_redshift_cluster, set_external_access
from sql_funcs import execute_single_sql

config = configparser.ConfigParser()
config.read_file(open('configs/aws.cfg'))
S3_IAM_ROLE_NAME = config.get("S3", "S3_IAM_ROLE_NAME")
S3_BUCKET_NAME = config.get("S3", "S3_BUCKET_NAME")
REDSHIFT_IAM_ROLE_NAME = config.get("REDSHIFT", "REDSHIFT_IAM_ROLE_NAME")
REDSHIFT_CLUSTER_IDENTIFIER = config.get("REDSHIFT", "REDSHIFT_CLUSTER_IDENTIFIER")
REDSHIFT_NODE_TYPE = config.get("REDSHIFT", "REDSHIFT_NODE_TYPE")
REDSHIFT_CLUSTER_TYPE = config.get("REDSHIFT", "REDSHIFT_CLUSTER_TYPE")
REDSHIFT_NUM_NODES = config.get("REDSHIFT", "REDSHIFT_NUM_NODES")
REDSHIFT_DB = config.get("REDSHIFT", "REDSHIFT_DB")
REDSHIFT_PORT = config.get("REDSHIFT", "REDSHIFT_PORT")
REDSHIFT_USER = config.get("REDSHIFT", "REDSHIFT_USER")
REDSHIFT_PASSWORD = config.get("REDSHIFT", "REDSHIFT_PASSWORD")
REDSHIFT_ENDPOINT = config.get("REDSHIFT", "REDSHIFT_ENDPOINT")
REDSHIFT_ROLE_ARN = config.get("REDSHIFT", "REDSHIFT_ROLE_ARN")
REDSHIFT_VPC_ID = config.get("REDSHIFT", "REDSHIFT_VPC_ID")

iam = boto3.client('iam')
s3 = boto3.resource('s3')
redshift = boto3.client('redshift')
ec2 = boto3.resource('ec2')

print('---- Setting roles and policies ----')
set_role_policy(
    iam_client=iam,
    role_name=S3_IAM_ROLE_NAME,
    role_description='test_s3',
    role_service='s3.amazonaws.com',
    policy_arns=['AmazonS3FullAccess'])


set_role_policy(
    iam_client=iam,
    role_name=REDSHIFT_IAM_ROLE_NAME,
    role_description='test_redshift',
    role_service='redshift.amazonaws.com',
    policy_arns=['AmazonS3ReadOnlyAccess'])


REDSHIFT_ROLE_ARN = iam.get_role(RoleName=REDSHIFT_IAM_ROLE_NAME)['Role']['Arn']

print('---- Creating a Redshift Cluster ----')
create_redshift_cluster(
    redshift_conn=redshift,
    identifier=REDSHIFT_CLUSTER_IDENTIFIER,
    node_type=REDSHIFT_NODE_TYPE,
    cluster_type=REDSHIFT_CLUSTER_TYPE,
    number_nodes=REDSHIFT_NUM_NODES,
    dbname=REDSHIFT_DB,
    username=REDSHIFT_USER,
    password=REDSHIFT_PASSWORD,
    role_arn=REDSHIFT_ROLE_ARN
)

while True:
    # Generally take 4~5 minutes
    cluster = redshift.describe_clusters(ClusterIdentifier=REDSHIFT_CLUSTER_IDENTIFIER)
    status = cluster.get('Clusters')[0].get('ClusterStatus')
    print('Cluster status: {}'.format(status))
    if status == 'available':
        break
    time.sleep(30)

REDSHIFT_ENDPOINT = redshift.describe_clusters(ClusterIdentifier=REDSHIFT_CLUSTER_IDENTIFIER).get('Clusters')[0]['Endpoint']['Address']
REDSHIFT_VPC_ID = redshift.describe_clusters(ClusterIdentifier=REDSHIFT_CLUSTER_IDENTIFIER).get('Clusters')[0]['VpcId']

set_external_access(ec2_conn=ec2, vpc_id=REDSHIFT_VPC_ID, redshift_port=REDSHIFT_PORT)

print('---- Creating a S3 Bucket ----')
S3_BUCKET_NAME = create_bucket(s3, 'mark')
time.sleep(5)

print('---- Uploading a file ----')
upload_file(s3, './data', S3_BUCKET_NAME)
time.sleep(5)

print('---- Creating table ----')
script = open("./sql/1.create_table.sql", "r").read()
execute_single_sql(dbname=REDSHIFT_DB,
                   host=REDSHIFT_ENDPOINT,
                   port=REDSHIFT_PORT,
                   user=REDSHIFT_USER,
                   password=REDSHIFT_PASSWORD,
                   sql=script)

print('---- Transferring data from S3 to Redshift ----')
script = open("./sql/2.fill_tables.sql", "r").read()
script = script\
    .replace('{%aws_ian%}', REDSHIFT_ROLE_ARN)\
    .replace('{%bucket_name%}', S3_BUCKET_NAME)
execute_single_sql(dbname=REDSHIFT_DB,
                   host=REDSHIFT_ENDPOINT,
                   port=REDSHIFT_PORT,
                   user=REDSHIFT_USER,
                   password=REDSHIFT_PASSWORD,
                   sql=script)



##############
#print('---- Deleting resources ----')
#delete_bucket_with_files(s3, S3_BUCKET_NAME)
#delete_redshift_cluster(redshift_conn=redshift, identifier=REDSHIFT_CLUSTER_IDENTIFIER)


