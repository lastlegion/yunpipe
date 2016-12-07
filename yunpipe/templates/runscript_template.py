import json
import os
import logging
from subprocess import call
import traceback
from time import sleep
from random import randint

import boto3.session
import botocore.exceptions

ALL_INPUTS = '%(inputs)s'
ALL_OUTPUTS = '%(outputs)s'
BASE_COMMAND = '%(baseCommand)s'

MAX_TRY = 5
DOWNLOAD_FAILED = False
UPLOAD_FAILED = False


REGION = os.getenv('AWS_DEFAULT_REGION')

# User sepcified informations
info = json.loads(os.getenv('info'))

metas = {'step', 'jobid', 'intermediate_s3', 'db_table', 'outputs'}

# sample info
# {
#     'step': '',
#     'jobid': '',
#     'intermediate_s3': '',
#     'db_table': '',
#     'inp': '',
#     'outputs': {}
# }


logger = logging.getLogger()
log_lvl = os.getenv('LOG_LVL', default='WARNING')

if log_lvl == 'ERROR':
    logger.setLevel(logging.ERROR)
elif log_lvl == 'INFO':
    logger.setLevel(logging.INFO)
elif log_lvl == 'DEBUG':
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.WARNING)

logger.addHandler(logging.StreamHandler())


session = boto3.session.Session(region_name=REGION)
s3 = session.client('s3')
db = session.resource('dynamodb')
table = db.Table(info['db_table'])


def download_file(para, value):
    '''
    if a parameter in the info message passed in is a FILE or FOLDER,
    down load the file or folder
    '''
    bucket = value['bucket']
    key = value['key']
    if 'rename' in ALL_INPUTS[para]:
        file = ALL_INPUTS[para]['rename']
    else:
        file = key

    for _ in range(0, MAX_TRY):
        try:
            s3.download_file(bucket, key, file)
            logger.info('donwloaded file')
            break
        except botocore.exceptions.ClientError as err:
            sleep(randint(1, 30))
            logger.debug(traceback.format_exc())
            logger.warn(err.response)
    else:
        global DOWNLOAD_FAILED
        DOWNLOAD_FAILED = True

    return file


# TODO
def download_folder(s3_folder, folder):
    '''
    download all the files in the s3 bucket to folder
    '''
    pass


def generate_command(paras, command):
    '''
    based on the new information, generate run command
    :para paras: input parameters passed into container
    :type: dict

    :para command:
    :type: string

    :rtype: list of string for the command
    sample inputs:

    baseCommand: echo
    inputs:
      example_flag:
        type: boolean
        inputBinding:
          position: 1
          prefix: -f
      example_string:
        type: string
        inputBinding:
          position: 3
          prefix: --example-string
      example_int:
        type: int
        inputBinding:
          position: 2
          prefix: -i
          separate: false
      example_file:
        type: File?
        inputBinding:
          prefix: --file=
          separate: false
          position: 4
    '''
    var = []
    for key in paras:
        if ALL_INPUTS[key]['type'] == 'boolean':
            if paras[key] is True:
                var.insert(ALL_INPUTS[key]['inputBinding']['position'],
                           ALL_INPUTS[key]['inputBinding']['prefix'])
        elif 'rename' in ALL_INPUTS[key]:
            pass
        elif 'inputBinding' in ALL_INPUTS[key]:
            if 'separate' in ALL_INPUTS[key]['inputBinding']:
                if ALL_INPUTS[key]['inputBinding']['separate'] is False:
                    s = ALL_INPUTS[key]['inputBinding']['prefix'] + paras[key]
                    var.insert(ALL_INPUTS[key]['inputBinding']['position'], s)
            else:
                s = ALL_INPUTS[key]['inputBinding']['prefix'] + ' ' + paras[key]
                var.insert(ALL_INPUTS[key]['inputBinding']['position'], s)

    if len(var) != 0:
        command = BASE_COMMAND + ' ' + ' '.join(var)
    else:
        command = BASE_COMMAND

    return command.split()


def run_program(paras):
    run_command = generate_command(paras, BASE_COMMAND)

    call(run_command)


# TODO
def get_output(output_expression):
    '''
    according ourput_expression, find the real outputs
    '''
    return output_expression


def upload_file(para, output_file, outputs_info):
    bucket = info['intermediate_s3']
    key = info['jobid'] + '/' + info['step'] + '/' + output_file
    for _ in range(0, MAX_TRY):
        try:
            s3.upload_file(output_file, bucket, output_file)
            logger.info('uploaded file')
            break
        except botocore.exceptions.ClientError as err:
            sleep(randint(1, 30))
            logger.debug(traceback.format_exc())
            logger.warn(err.response)
    else:
        global UPLOAD_FAILED
        UPLOAD_FAILED = True
        return
    outputs_info[para] = {}
    outputs_info[para]['key'] = key
    outputs_info[para]['bucket'] = bucket


# TODO
def upload_folder(para, output_file, outputs_info):
    pass


if __name__ == '__main__':
    # get inputs
    paras = {}
    for key in info:
        if key not in metas:
            if ALL_INPUTS[key]['type'] == 'FILE':
                paras[key] = download_file(key, info[key])
                if DOWNLOAD_FAILED is True:
                    # logging
                    exit(1)
            elif ALL_INPUTS[key]['type'] == 'FOLDER':
                paras[key] = download_folder(key, info[key])
                if DOWNLOAD_FAILED is True:
                    # logging
                    exit(1)
            else:
                paras[key] = info[key]

    # run program
    run_program(paras)

    # upload outputs
    outputs_info = {}
    for key in info['outputs']:
        if ALL_OUTPUTS[key]['type'] == 'FILE':
            output_file = get_output(ALL_OUTPUTS[key]['outputBinding'])
            upload_file(key, output_file, outputs_info)
            if UPLOAD_FAILED is True:
                # logging
                exit(1)
        elif ALL_OUTPUTS[key]['type'] == 'FOLDER':
            output_file = get_output(ALL_OUTPUTS[key]['outputBinding'])
            upload_folder(key, output_file, outputs_info)
            if UPLOAD_FAILED is True:
                # logging
                exit(1)
        else:
            # TODO
            pass

    # update dynamoDB
    expression = 'SET ' + info['step'] + ' = :val'
    updated = False
    while not updated:
        try:
            table.update_item(
                Key={'jobid': info['jobid']},
                UpdateExpression=expression,
                ExpressionAttributeValues={':val': outputs_info})
            updated = True
        except botocore.exceptions.ClientError as err:
            sleep(randint(3, 30))
