import sys
from random import random

from . import session
from .set_pipe_with_db import gettime
from ..cwl_parser import parse_job
from .. import get_workflow_info


db = session.resource('dynamodb')


def main():
    wf_name = sys.argv[1]
    jobpath = sys.argv[2]

    # TODO: based on wf_name find work_flow
    wf = get_workflow_info(wf_name)
    job = parse_job(jobpath)
    jobid = gettime() + str(random())

    table = db.Table(wf['db_table'])

    table.put_item(Item={
        'jobid': jobid,
        'intermediate_s3': wf['intermediate_s3'],
        'db_table': wf['db_table']
    })

    table.update_item(
        Key={'jobid': jobid},
        UpdateExpression='SET inputs = :val',
        ExpressionAttributeValues={':val': job}
    )
