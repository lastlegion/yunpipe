'''
Utility functions for yunpipe
'''

import os.path
import os
import errno
import json

# from . import YUNPIPE_WORKFLOW_FOLDER


def get_full_path(path):
    '''
    convert a relative path to absolute path.
    '''
    return os.path.abspath(os.path.expanduser(path))


def create_folder(folder):
    '''
    create folder if not existed
    '''
    try:
        os.makedirs(folder)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


def get_true_or_false(message, default=False):
    '''
    transfer user input Y/n into True or False

    :para message: input message should to user
    :type: string

    :para default: default value

    :rtype: boolean
    '''
    expected_response = {'y', 'Y', 'n', 'N', ''}
    response = input(message)
    while response not in expected_response:
        response = input(message)

    if response == 'Y' or response == 'y':
        return True
    elif response == 'N' or response == 'n':
        return False
    else:
        return default


def get_int(message, default):
    '''
    transfer user input to int numbers. Continue asking unless valid input.
    If user omit the input and default is set to non-None, get default number
    instand.

    :para message: input message should to user
    :type: string

    :para default: default value

    :rtype: int
    '''
    while True:
        response = input(message)
        if response == '':
            if default is None:
                print('Please input an integer value.')
                continue
            else:
                return default
        else:
            try:
                return int(response)
            except ValueError:
                print('Please input integer value.')


def get_workflow_info(name):
    '''
    retrieve work flow information using defined work flow name
    :para: name: the name of the work flow
    :type: string

    rtype: dictionary
    '''
    filepath = os.path.join(YUNPIPE_WORKFLOW_FOLDER, name + '.Workflow')
    with open(filepath, 'r') as f:
        res = json.load(f)
    return res
