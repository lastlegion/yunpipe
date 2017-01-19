import json
from zipfile import ZipFile
import sys
import os
from time import gmtime, strftime
from copy import deepcopy

from botocore.exceptions import ClientError
from haikunator import Haikunator

from .image_class import image
from .task_config import get_task_credentials
from . import session
from .. import CLOUD_PIPE_ALGORITHM_FOLDER
from .. import CLOUD_PIPE_TMP_FOLDER
from .. import CLOUD_PIPE_TEMPLATES_FOLDER
from .. import YUNPIPE_WORKFLOW_FOLDER
from ..cwl_parser import parse_workflow


name_generator = Haikunator()

LAMBDA_EXEC_TIME = 300
LAMBDA_EXEC_ROLE_NAME = 'lambda_exec_role'
LAMBDA_DYNAMO_STREAM_ROLE = 'lambda_dynamo_streams'

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

def get_image_info(name):
    '''
    based on the name of the user request, find the image inforomation
    para name: algorithm name
    type: string

    rpara: the infomation of a algorithm, see
    rtype: image_class.image_info
    '''
    # TODO: need to be rewrite down the road
    file_name = name + '.commandLineTool'
    file_path = os.path.join(CLOUD_PIPE_ALGORITHM_FOLDER, file_name)
    with open(file_path, 'r') as tmpfile:
        info = image(json.load(tmpfile))
    return info


def generate_task_definition(image_info, credentials):
    '''
    Based on the algorithm information and the user running information,
    generate task definition
    para image_info: all the required info for running the docker container
    type: image_info class
    para: user_info: passed in information about using the algorithm.
    user_info: {'port' : [], 'variables' = {}}
    type: json

    rtype json
    {
        'taskDefinition': {
            'taskDefinitionArn': 'string',
            'containerDefinitions': [...],
            'family': 'string',
            'revision': 123,
            'volumes': [
                {
                    'name': 'string',
                    'host': {
                        'sourcePath': 'string'
                    }
                },
            ],
            'status': 'ACTIVE'|'INACTIVE',
            'requiresAttributes': [
                {
                    'name': 'string',
                    'value': 'string'
                },
            ]
        }
    }
    '''
    image_info.init_all_variables(credentials)
    task_def = image_info.generate_task()
    task = session.client('ecs').register_task_definition(family=task_def[
        'family'], containerDefinitions=task_def['containerDefinitions'])
    # task name: task_def['family']
    return task


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
def generate_lambda_db_code(required_steps, work_flow):
    '''
    '''
    file_path = os.path.join(
        CLOUD_PIPE_TEMPLATES_FOLDER, 'lambda_db.txt')
    with open(file_path, 'r') as tmpfile:
        code = tmpfile.read()
    return code % {'steps': required_steps, 'work_flow': work_flow}


def generate_lambda_code_trigger_ecs(image, sys_info, task):
    '''
    generate lambda function code using lambda_run_task_template
    para: image: the informations about using a image
    type: image_class.image_info

    para: sys_info: other system info, see _get_sys_info()
    type: dict

    rtype: string
    '''
    lambda_para = {}
    lambda_para['instance_type'] = image.instance_type
    lambda_para['memory'] = image.memory
    lambda_para['task_name'] = task['taskDefinition']['family']
    lambda_para['container_name'] = task['taskDefinition']['containerDefinitions'][0]['name']
    lambda_para.update(sys_info)
    file_path = os.path.join(CLOUD_PIPE_TEMPLATES_FOLDER,
            'lambda_run_task_template.txt')
    with open(file_path, 'r') as tmpfile:
        code = tmpfile.read()
    return code % lambda_para


def create_deploy_package(lambda_code, zipname):
    '''
    generate the deploy package
    '''
    file_path = os.path.join(CLOUD_PIPE_TMP_FOLDER, 'lambda_function.py')
    with open(file_path, 'w+') as run_file:
        run_file.write(lambda_code)
    with ZipFile(zipname, 'w') as codezip:
        codezip.write(file_path, arcname='lambda_function.py')
    os.remove(file_path)

# TODO: config to correct role
def create_lambda_func(zipname, policy_role):
    '''
    create lambda function using a .zip deploy package
    '''
    # code = io.BytesIO()
    # with ZipFile(code, 'w') as z:
    #     with ZipFile(zipname, 'r') as datafile:
    #         for file in datafile.namelist():
    #             z.write(file)
    with open(zipname, 'rb') as tmpfile:
        code = tmpfile.read()
    name = name_generator.haikunate()
    role = _get_role_arn(policy_role)
    res = session.client('lambda').create_function(FunctionName=name, Runtime='python2.7', Role=role, Handler='lambda_function.lambda_handler', Code={'ZipFile': code}, Timeout=LAMBDA_EXEC_TIME, MemorySize=128)

    os.remove(zipname)

    return res['FunctionArn']


def gettime():
    return strftime('%Y-%m-%d_%H-%M-%S', gmtime())


#dynamo db
def create_db(table_name, lambda_arn):
    '''
    create table with name table_name for tracking progress
    '''
    table = session.resource('dynamodb').create_table(
        TableName=table_name,
        KeySchema=[{'AttributeName': 'jobid', 'KeyType': 'HASH'}],
        AttributeDefinitions=[{'AttributeName': 'jobid', 'AttributeType': 'S'}],
        ProvisionedThroughput={'ReadCapacityUnits': 3, 'WriteCapacityUnits': 1},
        StreamSpecification={'StreamEnabled': True, 'StreamViewType': 'NEW_AND_OLD_IMAGES'}
    )

    print(table.latest_stream_arn)

    # add dynamoDB Stream to lambda
    session.client('lambda').create_event_source_mapping(
        EventSourceArn=table.latest_stream_arn,
        FunctionName=lambda_arn,
        BatchSize=1,
        StartingPosition='TRIM_HORIZON'
    )


# utils
def get_sys_info():
    '''
    prepare the system information (non-task specific informations) including
    ec2 image_id, key_pair, security_group, subnet_id, iam_name, region,
    accout_id for making the lambda function.

    rtype dict
    '''
    # TODO: need rewrite this function
    info = {}
    info['image_id'] = 'ami-a58760b3'
    info['iam_name'] = 'ecsInstanceRole'
    info['subnet_id'] = 'subnet-d32725fb'
    info['security_group'] = 'default'
    info['key_pair'] = "wyx-aws"
    info['region'] = "us-east-1"
    info['account_id'] = "183351756044"
    return info


def main():
    wf = parse_workflow(sys.argv[1])
    print(json.dumps(wf, indent=4, sort_keys=True))

    wf_info = {}
    wf_info['inputs'] = wf['inputs']
    wf_info['outputs'] = wf['outputs']
    wf_info['output_s3'] = wf['output_s3']
    wf_info['intermediate_s3'] = wf['intermediate_s3']

    # create_db(wf['name'] + '_' + gettime())

    wf_for_lambda = {}
    # fix wf_for_lambda['outputs'] later
    wf_for_lambda['outputs'] = {}
    wf_for_lambda['outputs']['out'] = []
    wf_for_lambda['steps'] = {}

    required_steps = {}
    for step in wf['steps']:
        # set up ecs_task
        cred = get_task_credentials()
        print(json.dumps(cred))

        image = get_image_info(step)

        task = generate_task_definition(image, cred)
        print(task)

        # generate lambda to trigger ecs
        sys_info = get_sys_info()
        code = generate_lambda_code_trigger_ecs(image, sys_info, task)
        zipname = os.path.join(CLOUD_PIPE_TMP_FOLDER, step + name_generator.haikunate() + '.zip')
        create_deploy_package(code, zipname)
        lambda_arn = create_lambda_func(zipname, LAMBDA_EXEC_ROLE_NAME)

        # build information for lambda_db
        wf_for_lambda['steps'][step] = deepcopy(wf['steps'][step])
        wf_for_lambda['steps'][step]['run'] = lambda_arn
        required_steps[step] = set()
        for key in wf['steps'][step]['inputs']:
            required_steps[step].add(wf['steps'][step]['inputs'][key].split('/')[0][1:])

        print('---- Successfully set up step {}----'.format(step))

    print(str(wf_for_lambda))
    print(required_steps)

    # generate lambda to process dynamodb stream
    code = generate_lambda_db_code(required_steps, wf_for_lambda)
    zipname = os.path.join(
        CLOUD_PIPE_TMP_FOLDER, step + name_generator.haikunate() + '.zip')
    create_deploy_package(code, zipname)
    lambda_db_arn = create_lambda_func(zipname, LAMBDA_DYNAMO_STREAM_ROLE)

    wf_info['db_table'] = wf['name'] + gettime()

    print('---- Successfully set up lambda function {} ----'.format(lambda_db_arn))

    create_db(wf_info['db_table'], lambda_db_arn)

    print('---- Successfully set up dynamodb and lambda linkage ----')

    filepath = os.path.join(
        YUNPIPE_WORKFLOW_FOLDER,
        wf['name'] + '.' + wf['class'])

    with open(filepath, 'w+') as f:
        json.dump(wf_info, f, indent=4, sort_keys=True)

    print('---- Successfully set up work flow {} ----'.format(wf['name']))

    # setup dynamodb, attach lambda function to it

    # set up ecs_task
    # cred = get_task_credentials()
    # print(json.dumps(cred))

    # info = get_image_info('sum')

    # task = generate_task_definition(info, cred)
    # print(task)
