def generate_tracking(workflow):
    '''
    input a json format workflow, output required previous steps for each
    new step

    :para workflow: dict
    :type: dict

    :return: required steps before running a new step
    :rtype: dict

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
            'inputs': {'inp': '#inp'},
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
            'inputs': {'out': {'out'}},
            'lambda_arn': ''
        }
    }
}
    '''
    res = {}
    for step in workflow['steps']:
        res[step] = set()
        for key in workflow['steps'][step]['inputs']:
            res[step].add(workflow['steps'][step]['inputs'][key].split('/')[0][1:])
    return res
