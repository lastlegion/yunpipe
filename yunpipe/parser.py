from yaml import safe_load
from copy import deepcopy

'''
parse descriptions of single algorithmic step or workflow to a dictionary that
is used internally
'''


def cwl_parser(filepath):
    '''
    :para filepath: a cwl file to describe either single algorithmic step or a
    workflow return a dictionary that used internally
    :type: file

    :rtype: dict
    '''
    with open('filepath') as f:
        raw = safe_load(f)

    info = {}
    for key in raw:
        if key == 'inputs' or key == 'outputs':
            info[key] = {}
            for entry in raw[key]:
                info[key][entry['id']] = \
                    {i: entry[i] for i in entry if i != 'id'}
        elif key == 'hints':
            for i in raw[key]:
                info[i] = deepcopy(raw[key][i])
        else:
            info[key] = deepcopy(raw[key])
    return info
