import boto3
import json
from __future__ import print_function

#
required_steps = {}
required_steps['sum'] = {'sq'}
required_steps['count'] = {'sq'}
required_steps['avg'] = {'sum', 'count'}
required_steps['out'] = {'avg', 'sum', 'count'}

#


work_flow = {
    'inputs': {

    },
    'outputs': {

    },
    'steps': {
        'sq': {
            'inputs': {'': ''},
            'outputs': {'out'}
        },
        'sum': {
            'inputs': {'inp': '#sq/out'},
            'outputs': {'out'}
        },
        'count': {
            'inputs': {'inp': '#sq/out'},
            'outputs': {'out'}
        },
        'avg': {
            'inputs': {'sum': '#sum/out', 'count': '#count/out'},
            'outputs': {'out'}
        },
        'out': {
            'inputs': {}
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
    for key in newImage.keys():
        if key not in oldImage:
            res[key] = newImage[key]
    return res


def find_steps_to_start(newImage, newRecords):
    '''
    find new steps that could start based on newest update.
    '''
    res = []
    for key in newRecords.keys():
        for step in required_steps.keys():
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
        for id, source in work_flow['steps'][step]['inputs']:
            tmp = source.split('/')
            info[id] = newImage[tmp[0][1:]][tmp[1]]
        # start lambda function
        for k, v in info:
            print(k)
            print(v)
