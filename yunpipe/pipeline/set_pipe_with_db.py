import json
from zipfile import ZipFile
import sys
import os
from time import gmtime, strftime

from botocore.exceptions import ClientError
from haikunator import Haikunator

from .image_class import image
from .task_config import get_task_credentials
from . import session
from .. import CLOUD_PIPE_ALGORITHM_FOLDER
from .. import CLOUD_PIPE_TMP_FOLDER
from .. import CLOUD_PIPE_TEMPLATES_FOLDER


LAMBDA_EXEC_ROLE_NAME = 'lambda_exec_role'

LAMBDA_EXEC_ROLE = {
    "Statement": [
        {
            "Action": [
                "logs:*",
                "cloudwatch:*",
                "lambda:invokeFunction",
                "sqs:SendMessage",
                "ec2:Describe*",
                "ec2:StartInsatnces",
                "iam:PassRole",
                "ecs:RunTask"
            ],
            "Effect": "Allow",
            "Resource": [
                "arn:aws:logs:*:*:*",
                "arn:aws:lambda:*:*:*:*",
                "arn:aws:sqs:*:*:*",
                "arn:aws:ec2:*:*:*",
                "arn:aws:cloudwatch:*:*:*",
                "arn:aws:ecs:*:*:*"
            ]
        }
    ],
    "Version": "2012-10-17"
}


LAMBDA_EXECUTION_ROLE_TRUST_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "",
            "Effect": "Allow",
            "Principal": {
                "Service": "lambda.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}

# S3 set up
def _is_s3_exist(name):
    '''
    check for existense
    '''
    s3 = session.client('s3')
    for bucket in s3.list_buckets()['Buckets']:
        if name == bucket['Name']:
            return True
    return False


def _get_or_create_s3(name):
    '''
    create s3 bucket if not existed
    rtype: string
    '''
    if not _is_s3_exist(name):
        session.client('s3').create_bucket(Bucket=name)
        print('create s3 bucket %s.' % name)
    else:
        print('find s3 bucket %s.' % name)
    return name


# ecs
# TODO: generate/retrieve task definition



# iam
# Did not understand what am I doing here...
def create_lambda_exec_role(role, role_name):
    '''
    create a new lambda_exec_role with policy_name using policy
    :para role: lambda run policy
    :type: dict

    :para role_name:
    :type: String
    '''
    # create role
    iam = session.client('iam')
    policy = json.dumps(LAMBDA_EXECUTION_ROLE_TRUST_POLICY, sort_keys=True)

    try:
        res = iam.get_role(role_name)
        _policy = res['Role']['AssumeRolePolicyDocument']
        if _policy is not None and json.dumps(_policy) == policy:
            pass
        else:
            iam.update_assume_role_policy(
                RoleName=role_name, PolicyDocument=policy)

    except ClientError:
        print('creating role %s', role_name)
        iam.create_role(RoleName=role_name,
                        AssumeRolePolicyDocument=role)
        res = iam.get_role(role_name)

    # add policy to the role
    exec_policy = json.dumps(role, sort_keys=True)

    res = iam.list_role_policies(RoleName=role_name)

    for name in res['PolicyNames']:
        if name == 'LambdaExec':
            break
    else:
        iam.put_role_policy(RoleName=role_name, PolicyName='LambdaExec', PolicyDocument=exec_policy)


def _get_role_arn(role_name):
    '''
    create the lambda execution role.
    '''
    try:
        res = session.client('iam').get_role(RoleName=role_name)
    except ClientError as e:
        print(e)
        print('Does not have role %s, make sure you have permission on creating iam role and run create-lambda-exec-role()', role_name)

    return res['Role']['Arn']


# lambda

def gettime():
    return strftime('%Y-%m-%d_%H:%M:%s', gmtime())


def create_db(table_name):
    '''
    create table with name table_name for tracking progress
    '''
    session.resource('dynamodb').create_table(
        TableName=table_name,
        KeySchema=[{'AttributeName': 'jobid', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'jobid', 'AttributeType': 'S'}],
        ProvisionedThroughput={'ReadCapacityUnits': 3, 'WriteCapacityUnits': 1},
        StreamSpecification={'StreamEnabled': True, 'StreamViewType': 'NEW_AND_OLD_IMAGES'}
    )

    # add dynamoDB Stream to lambda
    session.client('lambda').create_event_source_mapping(
        EventSourceArn='',
        FunctionName='',
        BatchSize=1,
        StartingPosition='TRIM_HORIZON'
    )

