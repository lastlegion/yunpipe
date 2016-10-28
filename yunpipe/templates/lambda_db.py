from __future__ import print_function

import boto3
import json


# needs to be generated
required_steps = {}
required_steps['precompute'] = {'inputs'}
required_steps['sum'] = {'precompute'}
required_steps['avg'] = {'precompute'}
required_steps['outputs'] = {'avg', 'sum', 'precompute'}

# needs to be generated
work_flow = {
    'inputs': {
        'inp'
    },
    'outputs': {
        'out': ['#avg/out', '#sq/out', '#precompute/cb']
    },
    'steps': {
        'precompute': {
            'inputs': {'inp': '#inputs/inp'},
            'outputs': ['sq', 'cb'],
            'lambda_arn': 'arn:aws:lambda:us-east-1:183351756044:function:start_precompute'
        },
        'sum': {
            'inputs': {'inp': '#precompute/sq'},
            'outputs': ['out'],
            'lambda_arn': 'arn:aws:lambda:us-east-1:183351756044:function:start_sum'
        },
        'avg': {
            'inputs': {'inp': '#precompute/sq'},
            'outputs': ['out'],
            'lambda_arn': 'arn:aws:lambda:us-east-1:183351756044:function:start_avg'
        },
        'OUT': {
            'inputs': {'inp': '#outputs/out'},
            'lambda_arn': ''
        }
    }
}

metas = {}

def lambda_handler(event, context):
    '''
    '''
    print(json.dumps(event))
    record = event['Records'][0]
    if record['eventName'] == 'MODIFY':
        newRecords = find_new_records(record['dynamodb']['OldImage'], record['dynamodb']['NewImage'])
        startList = find_steps_to_start(record['dynamodb']['NewImage'], newRecords)
        start_docker_task(startList, record['dynamodb']['NewImage'])


def find_new_records(oldImage, newImage):
    '''
    find the new records in the modified data entry
    oldImage:
    newImage
    '''
    res = {}
    for key in newImage:
        if key not in oldImage:
            res[key] = newImage[key]
    print('New record:' + json.dumps(res))
    return res


def find_steps_to_start(newImage, newRecords):
    '''
    find new steps that could start based on newest update.
    '''
    res = []
    for key in newRecords:
        for step in required_steps:
            if key in required_steps[step]:
                has_every_steps = True
                for x in required_steps[step]:
                    has_every_steps = has_every_steps and x in newImage
                if has_every_steps is True:
                    res.append(step)
    return res


def start_docker_task(startList, newImage):
    for step in startList:
        info = {}
        info['step'] = step
        info['jobid'] = newImage['jobid']['S']
        info['intermediate_s3'] = newImage['intermediate_s3']
        info['db_table'] = newImage['db_table']['S']
        if step == 'OUT':
            # need to handle 'OUT' seperately
            # info['copy']: list of files to be copyed
            # info['delete']: list of files to be deleted

            # TODO:
            continue
        for para in work_flow['steps'][step]['inputs']:
            tmp = work_flow['steps'][step]['inputs'][para].split('/')
            info[para] = {}
            info[para]['bucket'] =\
                newImage[tmp[0][1:]]['M'][tmp[1]]['M']['bucket']['S']
            info[para]['key'] =\
                newImage[tmp[0][1:]]['M'][tmp[1]]['M']['key']['S']

        # TODO: handle outputs
        info['outputs'] = workflow['steps']['step']['outputs']
        # call lambda function to start ecs
        client = boto3.client('lambda')
        response = client.invoke(
            FunctionName=work_flow['steps'][step]['lambda_arn'],
            Payload=json.dumps(info).encode())
        while(response['StatusCode'] != 200):
            response = client.invoke(
                FunctionName=work_flow['steps'][step]['lambda_arn'],
                Payload=json.dumps(info).encode())
        print('started function ' + work_flow['steps'][step]['lambda_arn'])

        # addapt from previous lambda_run_task_template.py


old = {'jobid': '001'}
new = {'jobid': '001', 'inputs': {'inp': {'inputs input'}}}

new1 = {'jobid': '001', 'inputs': {'inp': {'inputs input'}},
        'sq': {'out': {'sq output'}}}
