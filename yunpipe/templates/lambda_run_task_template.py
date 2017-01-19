from __future__ import print_function
import boto3
import json
from time import time

print('Loading lambda function')

sqs = boto3.client('sqs')
ecs = boto3.client('ecs')
ec2 = boto3.client('ec2')
cw = boto3.client('cloudwatch')


# maintain a set of container instance arn that isnot the instance_type wanted
instances = set()

ec2InstanceId = ''

# def find_container(cluster, instance_id):
#     while True:
#         ins = ecs.list_container_instances(cluster=cluster)
#         ins = ecs.describe_container_instances(
#             cluster=cluster, containerInstances=ins['containerInstanceArns'])
#         ins = ins['containerInstances']
#         for info in ins:
#             if info['ec2InstanceId'] == instance_id:
#                 return info['containerInstanceArn']
#         # print('run {} times'.format(i))
#         # i += 1


def get_instances_arns(cluster):
    '''
    find all the instances that have the instance_type we want
    '''
    arns = []
    response = ecs.list_container_instances(cluster=cluster)
    arns.extend(
        [i for i in response['containerInstanceArns'] if i not in instances])
    while response.get('nextToken', None) is not None:
        response = ecs.list_container_instances(
            cluster=cluster, nextToken=response['nextToken'])
        arns.extend(
            [i for i in response['containerInstanceArns']
                if i not in instances])

    return arns


def start_task(cluster, memory, run_info):
    '''
    given a cluster and task required memory, return true if successfully start
    task.
    '''
    global ec2InstanceId
    arns = get_instances_arns(cluster)
    ins = ecs.describe_container_instances(
        cluster=cluster, containerInstances=arns)['containerInstances']
    ec2_started = False
    for inc in ins:
        info = ec2.describe_instances(InstanceIds=[inc['ec2InstanceId']])
        if info['Reservations'][0]['Instances'][0]['InstanceType'] != '%(instance_type)s':
            instances.add(inc['containerInstanceArn'])
        else:
            if inc['ec2InstanceId'] == ec2InstanceId:
                ec2_started = True
            if inc['remainingResources'] >= memory:
                res = ecs.start_task(
                    taskDefinition='%(task_name)s',
                    containerInstances=[inc['containerInstanceArn']],
                    overrides={'containerOverrides':
                        [{
                            'name': '%(container_name)s',
                            'environment':
                            [{'name': 'info', 'value': run_info}]
                        }]})
                if len(res['failures']) == 0:
                    print('start tast at {}'.format(inc['containerInstanceArn']))
                    return True
    if ec2_started:
        ec2InstanceId = create_ec2()
        print('created ec2 has been used, start new ec2')
    return False


def create_ec2():
    '''
    add ec2 machine into ecs default cluster and add cloudwatch shut down
    :rtype: string
    '''
    # add ec2 machine into ecs default cluster
    ec2 = boto3.resource('ec2')
    instances = ec2.create_instances(ImageId='%(image_id)s', MinCount=1,
                                     KeyName='%(key_pair)s', MaxCount=1,
                                     # SecurityGroups=['%(security_group)s'],
                                     InstanceType='%(instance_type)s',
                                     SubnetId='%(subnet_id)s',
                                     IamInstanceProfile={'Name': '%(iam_name)s'})

    # registe instances for cloudwatch shutdown
    alarm_name = instances[0].id + '-shutdown'
    alarm_act = ['arn:aws:swf:%(region)s:%(account_id)s:action/actions/AWS_EC2.InstanceId.Terminate/1.0']
    dimension = [{"Name": "InstanceId", "Value": instances[0].id}]
    cw.put_metric_alarm(AlarmName=alarm_name, AlarmActions=alarm_act,
                        MetricName='CPUUtilization', Namespace='AWS/EC2',
                        Statistic='Average', Dimensions=dimension, Period=300,
                        EvaluationPeriods=2,
                        Threshold=1, ComparisonOperator='LessThanThreshold')

    # send the cloudwatch name and instance id for cleanup
    # message = {}
    # message['alarm_name'] = alarm_name
    # message['ec2InstanceId'] = instances[0].id
    # sqs.send_message(QueueUrl='(alarm_sqs)s', MessageBody=json.dumps(message))
    return instances[0].id


def lambda_handler(event, context):
    start_time = time()
    info = json.dumps(event)
    # print("Received event: " + json.dumps(event, indent=2))

    print('run time {}'.format((time() - start_time)))
    # start task at given type of instance
    if not start_task('default', %(memory)s, info):
        print('firest check run time {}'.format((time() - start_time)))
        ec2InstanceId = create_ec2()
        print('run time {}'.format((time() - start_time)))

        while not start_task('default', %(memory)s, info):
            pass

    print('run time {}'.format((time() - start_time)))
    return 'send messages to sqs and start ecs'
