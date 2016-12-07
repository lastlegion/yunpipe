import json
import argparse
import os
from os.path import join
from subprocess import call

from .. import CLOUD_PIPE_TEMPLATES_FOLDER
from .. import CLOUD_PIPE_TMP_FOLDER
from .. import CLOUD_PIPE_ALGORITHM_FOLDER
from .. import create_folder
from ..utils import get_int
from ..utils import get_true_or_false


SUPPORTED_SYSTEM = {'ubuntu'}

# TODO: change runscript_template: get info from info enviroment variable, update dynamoDB
# TODO: get inputs and outputs from algorithm definition
# TODO: 


def generate_dockerfile(system_name, container_name, work_dir):
    '''
    generate the dockerfile content.

    Based on the system which user's prebuild image, generate dockerfile
    including adding run enviroment of runscript.py and add runscript.

    :para system_name: the system name in which the docker image is built on
    :tpye: string

    :para container_name: user's algorithm container
    :tpye: string

    :return: the dockerfile content.
    :rtype: string
    '''
    if system_name == 'ubuntu':
        file_path = join(CLOUD_PIPE_TEMPLATES_FOLDER, 'ubuntu_wrapper.txt')
        with open(file_path, 'r') as myfile:
            dockerfile = myfile.read()
    return dockerfile % {'container_name': container_name,
                         'work_dir': work_dir}


def show_dockerfile(system_name, container_name, work_dir):
    print(generate_dockerfile(system_name, container_name, work_dir))


def generate_runscript(**kwargs):
    '''
    generate runscript that fetch information from sqs, handling
    download/upload file and run script.

    :para inputs: dict of input definition
    :type: dict

    :para outputs: output definition
    :type: string

    :para command: run command of user's algorithm script
    :type: string

    :return: the runscript for runscript.py. Include fetching information,
    download / upload file and run script.
    :rtype: string
    '''
    file_path = join(CLOUD_PIPE_TEMPLATES_FOLDER, 'runscript_template.txt')
    with open(file_path, 'r') as myfile:
        script = myfile.read()
    return script % kwargs


def show_runscript(input_path, output_path, name, command):
    print(generate_runscript(input_path, output_path, name, command))


def describe_algorithm():
    '''
    command line editor of the detailed information of algorithm container.

    :rtype: json
    '''
    info = {}
    info['container_name'] = input(
        'Please input your containerized algorithm name:\n')
    info['system'] = input(
        'Please input which operating system your container is build on:\n')
    info['run_command'] = input(
        'Please input the basic run command of your algorithm\n')
    # info['input_file_path'] = input(
    #     'Please input full path to the folder where input file should be:\n')
    # info['output_file_path'] = input(
    #     'Please input full path to the folder where output file should be:\n')
    info['name'] = input(
        'Please input the name you want other user refer your algorithm as:\n')
    info['instance_type'] = input(
        'Please input one instance type on aws best fit running your algorithm. You can omit this:\n')

    info['memory'] = {}
    info['memory']['minimal'] = get_int(
        'Please input the minimal memory requirement for running your algorithm in MB. You can omit this\n', 4)
    info['memory']['suggested'] = get_int(
        'Please input the suggested memory requirement for running your algorithm in MB:\n', None)
    info['CPU'] = get_int(
        'Please input the number of CPUs used for this algorithm. You can omit this if you already suggested an instance type.\n', 1)

    info['user_specified_environment_variables'] = []
    if get_true_or_false('Do you want to add variables you open to user? [y/n]'):
        addmore = True
        while addmore:
            helper = {}
            helper['name'] = input(
                'Please input the variable name you open to user:\n')
            helper['required'] = get_true_or_false(
                'Is this a required variable? [y/n]: ')
            addmore = get_true_or_false(
                'Do you want to add more variables? [y/n]: ')
            info['user_specified_environment_variables'].append(helper)

    info['port'] = []
    if get_true_or_false('Do you want to open port to user? [y/n]'):
        addmore = True
        while addmore:
            helper = {}
            response = ''
            port = get_int(
                'Please input the port number you open to user:\n', None)
            if port in info['port']:
                print('This port number has already been set\n')
                continue
            helper['port'] = port
            while response != 'tcp' and response != 'udp':
                response = input(
                    'Please input the protocol of the port: [tcp/udp]\n')
            helper['protocol'] = response
            addmore = get_true_or_false(
                'Do you want to add more ports? [y/n]:')
            info['port'].append(helper)

    # print(json.dumps(info, indent='    '))

    return info


def wrapper(alg_info):
    '''
    automatic generate dockerfile according to the information user provided.

    :para alg_info: a json object contains necessory information about
    algorithm
    :type: json
    '''

    # if alg_info['input_file_path'][-1] != '/':
    #     alg_info['input_file_path'] += '/'
    # if alg_info['output_file_path'][-1] != '/':
    #     alg_info['output_file_path'] += '/'

    # generate runscript
    # create a folder with name for dockerfile & runscript
    folder = join(CLOUD_PIPE_TMP_FOLDER, alg_info['name'])
    create_folder(folder)

    # generate runscript
    runscript = generate_runscript(inputs=alg_info['inputs'],
                                   outputs=alg_info['outputs'],
                                   command=alg_info['baseCommand'])

    run_file = join(folder, 'runscript.py')
    with open(run_file, 'w+') as tmpfile:
        tmpfile.write(runscript)

    # generate dockerfile
    if alg_info['system'] not in SUPPORTED_SYSTEM:
        print("not support %s yet." % alg_info['system'])
        return
    dockerfile = generate_dockerfile(
        alg_info['system'], alg_info['container_name'], alg_info['work_dir'])

    docker_file = join(folder, 'Dockerfile')
    with open(docker_file, 'w+') as tmpfile:
        tmpfile.write(dockerfile)


# TODO
def get_instance_type(alg_info):
    '''
    Based on the algorithm developer provided information, choose an
    apporperate ec2 instance_type

    :para alg_info: a json object contains necessory information about
    algorithm
    :type: json

    :rtype: sting of ec2 instance type
    '''
    # TODO: rewrite
    return 't2.micro'


def generate_image(name, folder_path, args):
    '''
    build new docker image and upload.

    giver new docker image name and dockerfile, build new image, tagged with
    user account and pushed to desired registry. Default registry is docker
    hub, will support other registry soon.

    :para name: new docker image name. Without tag and registry.
    :type: string

    :para folder_path: the path to tmp folder where stores dockerfiles.
    path is ~/.cloud_pipe/tmp/name
    :typr: string

    :para args: command line arguments passed in from scripts.wrap, currently
    only useful entry is user, will using registry soon
    :type: argparser object

    :rtpye: docker image with repo name
    '''
    # TODO: rewrite
    # PATH = '../algorithms/'
    # name = dockerfile_name.split('.')[0]
    tagged_name = args.user + '/' + name
    BUILD_COMMAND = 'docker build -t %(name)s %(path)s' \
        % {'name': name, 'path': join(folder_path, '.')}
    TAG_COMMAND = 'docker tag %(name)s %(tag)s' % {
        'tag': tagged_name, 'name': name}
    UPLOAD_COMMAND = 'docker push %(tag)s' % {'tag': tagged_name}

    print(BUILD_COMMAND)

    call(BUILD_COMMAND.split())
    call(TAG_COMMAND.split())
    call(UPLOAD_COMMAND.split())

    # remove the folder generated during the image generatation process
    remove = 'rm -r ' + folder_path
    # call(remove.split())

    return tagged_name


def generate_image_info(alg_info, container_name):
    '''
    generate wrapped image information for ecs task

    :para alg_info: algorthm information user provided
    :type: json

    :para container_name: access name of the wrapped container
    :type string

    rtype: json
    '''
    new_vars = []
    # new_vars.append({'name': 'output_s3_name', 'required': True})
    # new_vars.append({'name': 'sqs', 'required': True})
    new_vars.append({'name': 'LOG_LVL', 'required': False})
    new_vars.append({'name': 'NAME', 'required': True})
    new_vars.append({'name': 'AWS_DEFAULT_REGION', 'required': True})
    new_vars.append({'name': 'AWS_DEFAULT_OUTPUT', 'required': True})
    new_vars.append({'name': 'AWS_ACCESS_KEY_ID', 'required': True})
    new_vars.append({'name': 'AWS_SECRET_ACCESS_KEY', 'required': True})

    alg_info['container_name'] = container_name
    if alg_info['instance_type'] == '':
        alg_info['instance_type'] = get_instance_type(alg_info)
    alg_info['user_specified_environment_variables'].extend(new_vars)
    return alg_info


def generate_all(alg, args):
    '''
    generate dockerfile, build new image, upload to registry and generate
    detailed information of the new image

    :para alg: algorthm information user provided
    :type: json

    :para agrs: command line argument from script.wrap. args.user and 
    args.registry
    :type: argparser object
    '''
    wrapper(alg)

    path = join(CLOUD_PIPE_TMP_FOLDER, alg['name'])
    container_name = generate_image(alg['name'], path, args)

    info = generate_image_info(alg, container_name)

    name = container_name.split('/')[-1] + '_info.json'

    file_path = join(CLOUD_PIPE_ALGORITHM_FOLDER, name)

    with open(file_path, 'w') as data_file:
        json.dump(info, data_file, indent='    ', sort_keys=True)

    print('Successfully wrap container {}'.format(container_name))


if __name__ == '__main__':
    DEFAULT_USER = 'wangyx2005'

    parser = argparse.ArgumentParser(description='A tool to wrap your containers')

    parser.add_argument('-d', '--describe', action='store_true',
                        help='use command line editor to describe your algorithm')
    parser.add_argument('-f', '--files', nargs='+',
                        help='List json files to describe your algorithms')
    parser.add_argument('-s', '--show', action='store_true',
                        help='show described algorithm before generate new container')
    parser.add_argument('-u', '--user', action='store', default=DEFAULT_USER,
                        help='user name of docker hub account, default is {}'.format(DEFAULT_USER))

    args = parser.parse_args()

    if args.describe is True and args.files is not None or \
            args.describe is False and args.files is None:
        print('please use either -d or -f flag')
        exit(0)

    if args.describe:
        alg = describe_algorithm()

        if args.show:
            print(json.dumps(alg, indent='    ', sort_keys=True))

        if not get_true_or_false('Do you want to continue? [y/n]:', True):
            exit(0)

        generate_all(alg, args)

    else:
        for file_name in args.files:
            with open(file_name, 'r') as data_file:
                alg = json.load(data_file)

            generate_all(alg, args)
