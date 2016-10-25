from yaml import safe_load
from copy import deepcopy

'''
parse descriptions of single algorithmic step or workflow to a dictionary that is used internally
'''


def parse_commandLineTool(filepath):
    '''
    :para filepath: a cwl file to describe single algorithmic step return a dictionary that used internally
    :type: file

    :rtype: dict
    '''
    with open(filepath) as f:
        raw = safe_load(f)

    info = {}
    for key in raw:
        if key == 'inputs' or key == 'outputs':
            info[key] = {}
            for entry in raw[key]:
                info[key][entry['id']] = \
                    {i: entry[i] for i in entry if i != 'id'}
        elif key == 'baseCommand':
            info[key] = ' '.join(raw[key])
        elif key == 'hints':
            for entry in raw[key]:
                for k in entry:
                    info[k] = deepcopy(entry[k])
        else:
            info[key] = deepcopy(raw[key])
    return info


def parse_workflow(filepath):
    '''
    :para filepath: a cwl file to describe a workflow return a dictionary that used internally
    :type: file

    :rtype: dict
    '''
    with open(filepath) as f:
        raw = safe_load(f)

    info = {}
    for key in raw:
        if key == 'inputs' or key == 'outputs':
            info[key] = {}
            for entry in raw[key]:
                info[key][entry['id']] = \
                    {i: entry[i] for i in entry if i != 'id'}
        elif key == 'steps':
            info[key] = {}
            for entry in raw[key]:
                info[key][entry['id']] = {}
                for k in entry:
                    if k == 'inputs':
                        info[key][entry['id']][k] = {}
                        for e in entry[k]:
                            if '/' in e['source']:
                                info[key][entry['id']][k][e['id']] = \
                                    e['source']
                            else:
                                info[key][entry['id']][k][e['id']] = \
                                    '#GLOBAL/' + e['source'][1:]
                    elif k == 'outputs':
                        info[key][entry['id']][k] = set()
                        for e in entry[k]:
                            info[key][entry['id']][k].add(e['id'])
                    elif k == 'run':
                        info[key][entry['id']][k] = entry['run']

    # add new step OUT to the steps

    return info
