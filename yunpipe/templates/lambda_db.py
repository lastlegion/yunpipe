from __future__ import print_function

import boto3
import json


# needs to be generated
required_steps = {}
required_steps['sq'] = {'GLOBAL'}
required_steps['sum'] = {'sq'}
required_steps['count'] = {'sq'}
required_steps['avg'] = {'sum', 'count'}
required_steps['OUT'] = {'avg', 'sum', 'count'}

# needs to be generated
work_flow = {
    'inputs': {
        'inp'
    },
    'outputs': {
        'out'
    },
    'intermediate_s3': '',
    'steps': {
        'sq': {
            'inputs': {'inp': '#GLOBAL/inp'},
            'outputs': {'out'},
            'lambda_arn': ''
        },
        'sum': {
            'inputs': {'inp': '#sq/out'},
            'outputs': {'out'},
            'lambda_arn': ''
        },
        'count': {
            'inputs': {'inp': '#sq/out'},
            'outputs': {'out'},
            'lambda_arn': ''
        },
        'avg': {
            'inputs': {'sum': '#sum/out', 'count': '#count/out'},
            'outputs': {'out'},
            'lambda_arn': ''
        },
        'OUT': {
            'inputs': {'out': {'#GLOBAL/out'}},
            'lambda_arn': ''
        }
    }
}


def lambda_handler(event, context):
    '''
    '''
    record = event['Records']
    if record['eventName'] == 'MODIFY':
        newRecords = find_new_records(record['OldImage'], record['NewImage'])
        startList = find_steps_to_start(record['NewImage'], newRecords)
        start_docker_task(startList, record['NewImage'])


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
        info['jobid'] = newImage['jobid']
        if step == 'OUT':
            # need to handle 'OUT' seperately
            # info['copy']: list of files to be copyed
            # info['delete']: list of files to be deleted

            # TODO:
            break
        for para in work_flow['steps'][step]['inputs']:
            tmp = work_flow['steps'][step]['inputs'][para].split('/')
            info[para] = newImage[tmp[0][1:]][tmp[1]]

        # call lambda function to start ecs
        client = boto3.client('lambda')
        response = client.invoke(FunctionName=work_flow[step]['lambda_arn'],
                                 Payload=json.dumps(info).encode())
        while(response['StatusCode'] != 200):
            response = client.invoke(
                FunctionName=work_flow[step]['lambda_arn'],
                Payload=json.dumps(info).encode())

        # addapt from previous lambda_run_task_template.py


old = {'jobid': '001'}
new = {'jobid': '001', 'global': {'inp': {'global input'}}}

new1 = {'jobid': '001', 'global': {'inp': {'global input'}},
        'sq': {'out': {'sq output'}}}
