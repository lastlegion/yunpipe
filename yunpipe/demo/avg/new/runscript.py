import json
import os
import logging
from subprocess import call
import traceback
from time import sleep
from random import randomint

import boto3.session
import botocore.exceptions

ALL_INPUTS = 
{
    "inp": {
        "rename": "input.txt",
        "required": True,
        "type": "FILE"
    }
}

ALL_OUTPUTS = 
{
    "out": {
        "outputBinding": "out.txt",
        "type": "FILE"
    },
}

BASE_COMMAND = 'python3 compute.py'

MAX_TRY = 5
DOWNLOAD_FAILED = False
UPLOAD_FAILED = False

# user specified environment variable
# UPLOADBUCKET = os.getenv('output_s3_name')
# QUEUEURL = os.getenv('sqs')
REGION = os.getenv('AWS_DEFAULT_REGION')
# NAME = os.getenv('NAME', default='%(name)s')

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

# user define if want to zip the result file
# NEED_ZIP = os.getenv('ZIP', default='True')

# get input/output file location
# INPUT_PATH = '%(input)s'
# OUTPUT_PATH = '%(output)s'

# WORK_DIR = '%(WORK_DIR)s'


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
# sqs = session.client('sqs')
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
        except botocore.exceptions.ClientError as err:
            sleep(randomint(1, 30))
            logger.debug(traceback.format_exc())
            logger.warn(err.response)
    else:
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
        else:
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
    bucket = info['intermediate_s3'] + '/' + info['jobid'] + '/' + info['step']
    for _ in range(0, MAX_TRY):
        try:
            s3.upload_file(output_file, bucket, output_file)
            logger.info('uploaded file')
        except botocore.exceptions.ClientError as err:
            sleep(randomint(1, 30))
            logger.debug(traceback.format_exc())
            logger.warn(err.response)
    else:
        UPLOAD_FAILED = True


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
                    return
            elif ALL_INPUTS[key]['type'] == 'FOLDER':
                paras[key] = download_folder(key, info[key])
                if DOWNLOAD_FAILED is True:
                    # logging
                    return
            else:
                paras[key] = info[key]

    # run program
    run_program(paras)

    # upload outputs
    outputs_info = {}
    for key in info['outputs']:
        if ALL_OUTPUTS[key]['type'] == 'FILE':
            output_file = get_output(ALL_OUTPUTS[key]['output_binding'])
            upload_file(key, output_file, outputs_info)
            if UPLOAD_FAILED is True:
                # logging
                return
        elif ALL_OUTPUTS[key]['type'] == 'FOLDER':
            output_file = get_output(ALL_OUTPUTS[key]['output_binding'])
            upload_folder(key, output_file, outputs_info)
            if UPLOAD_FAILED is True:
                # logging
                return
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
            sleep(randomint(3, 30))


# def pull_files(message_URL):
#     '''
#     pull message form message_URL
#     '''
#     msgs = {}
#     while 'Messages' not in msgs:
#         logger.debug('pull message from SQS: {}'.format(message_URL))
#         while 'Messages' not in msgs:
#             try:
#                 msgs = sqs.receive_message(QueueUrl=message_URL)
#             except botocore.exceptions.ClientError as err:
#                 logger.debug(traceback.format_exc())
#                 logger.warn(err.response)
#                 continue
#             except Exception as err:
#                 logger.debug(traceback.format_exc())
#                 logger.error('Unexpected error occures at pull_files()!!')
#                 logger.error(err)
#                 continue

#         if '\"Records\"' not in msgs['Messages'][0]['Body']:
#             # delete such message
#             try:
#                 sqs.delete_message(QueueUrl=message_URL,
#                                    ReceiptHandle=msgs['Messages'][0]['ReceiptHandle'])
#             except botocore.exceptions.ClientError as err:
#                 logger.debug(traceback.format_exc())
#                 logger.warn(err.response)
#                 sqs.delete_message(QueueUrl=message_URL,
#                                    ReceiptHandle=msgs['Messages'][0]['ReceiptHandle'])
#             except Exception as err:
#                 logger.debug(traceback.format_exc())
#                 logger.error('Unexpected error occures when delete message!!')
#                 logger.error(err)
#             logger.info('delete one useless message from file queue')
#             msgs = {}
        
#     logger.info('receive file info from SQS')
#     for msg in msgs['Messages']:
#         record = json.loads(msg['Body'])['Records'][0]
#         bucket = record['s3']['bucket']['name']
#         file = record['s3']['object']['key']

#         # delete message
#         try:
#             sqs.delete_message(QueueUrl=message_URL,
#                                ReceiptHandle=msg['ReceiptHandle'])
#         except botocore.exceptions.ClientError as err:
#             logger.debug(traceback.format_exc())
#             logger.warn(err.response)
#             sqs.delete_message(QueueUrl=message_URL,
#                                ReceiptHandle=msg['ReceiptHandle'])
#         except Exception as err:
#             logger.debug(traceback.format_exc())
#             logger.error('Unexpected error occures when delete message!!')
#             logger.error(err)
#         logger.info('delete one message from file queue')

#         # download file from input S3 bucket
#         input_file = download_file(bucket, file, msg)
#         if input_file == '':
#             continue

#         # run program
#         result = run_program(input_file)

#         # upload file
#         upload_file(result, file)


# def download_file(bucket, file, msg):
#     file_name = file.split('/')[-1]
#     try:
#         s3.download_file(bucket, file, INPUT_PATH + file_name)
#         logger.info('donwloaded file')
#         return INPUT_PATH + file_name
#     except Exception as err:
#         # This part has problem. formate is wrong
#         # put the failed file info back to sqs
#         logger.warn(err)
#         logger.debug(traceback.format_exc())
#         logger.debug('message content: {}'.format(json.dumps(msg)))
#         logger.info('put message back to SQS for later try')
#         try:
#             sqs.send_message(QueueUrl=message_URL,
#                              MessageBody=json.dumps(msg))
#         except Exception as err:
#             logger.warn(err)
#             logger.debug(traceback.format_exc())
#             return ''
#         return ''


# def run_program(input_file):
#     command = '%(command)s'

#     file_name = input_file.split('/')[-1]
#     result_file = OUTPUT_PATH + 'Result-' + NAME + '-' + file_name

#     run_command = command.split()
#     for i in range(len(run_command)):
#         if run_command[i] == '$input':
#             run_command[i] = input_file
#         if run_command[i] == '$output':
#             run_command[i] = result_file

#     # with open(input_file, 'r') as f:
#     #     print(f.read())
#     # print(run_command)

#     call(run_command)

#     # check if need to zip
#     if os.path.isdir(result_file):
#         file_name = 'Result-' + NAME + '-' + file_name.split('.')[0] + '.zip'
#         call(['zip', '-rv9', file_name, result_file])
#     else:
#         file_name = result_file
#     return file_name


# def upload_file(file, input_file):
#     path = input_file.split('/')
#     path[-1] = file.split('/')[-1]
#     s3_key = '/'.join(path)
#     try:
#         s3.upload_file(file, UPLOADBUCKET, s3_key)
#     except botocore.exceptions.ClientError as err:
#         logger.warn(err)
#     except Exception as err:
#         logger.debug(traceback.format_exc())
#         logger.error('Unexpected error happend will upload file')
#         logger.err(err)


# if __name__ == '__main__':
    
