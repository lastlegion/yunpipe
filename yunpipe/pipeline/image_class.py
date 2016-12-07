import json
import os.path

from haikunator import Haikunator

from .. import CLOUD_PIPE_TEMPLATES_FOLDER

name_generator = Haikunator()

# TODO: add instance_type


class image:
    class port_class:
        def __init__(self, port_info):
            '''
            para: port_info: dict contains all port information
            type: dict
            '''
            self.host_port = None
            self.container_port = port_info['port']
            self.protocol = port_info['protocol']

        def add_default_port_mapping(self):
            self.host_port = self.container_port

    class variable_class:
        def __init__(self, var):
            '''
            para var: dict conatins all var information
            type: dict
            '''
            self.name = var['name']
            self.initialized = False
            self.required = var['required']
            self.value = None

            # TODO
            def valid_entry():
                pass

        def init_var(self, value):
            self.value = value
            self.initialized = True

    def __init__(self, info):
        '''
        para info:
        type: json
        '''
        self.memory = info['memory']['suggested']
        self.name = info['name']
        self.image = info['container_name']
        self.instance_type = info['instance_type']
        # TODO: this need to be changed
        self.cpu = 32
        self.port = {}
        self.env_variable = {}

        for port_info in info['port']:
            self.port[port_info['port']] = self.port_class(port_info)

        for var in info['user_specified_environment_variables']:
            self.env_variable[var['name']] = self.variable_class(var)

    def init_all_variables(self, info, credentials):
        '''
        Based on the user information provided, initialize all the entries
        para info:
        type: json
        '''
        for port_number in info['port']:
            self.port[port_number].add_default_port_mapping()

        # for name, value in info['variables'].items():
        #     self.env_variable[name].init_var(value)

        for name, value in credentials.items():
            self.env_variable[name].init_var(value)

    def valid_info(self):
        '''
        check if such initialization is vaild. Namely, check if all required
        variable has been initializaed, if every variable meet its requirement.

        rtype: boolean
        '''
        for name, var in self['env_variable'].items():
            if not var.vaild_entry():
                return False
            if var.required and var.initializaed:
                print("envoriment variable %s needs to be initialized", name)
                return False
        return True

    def add_required_variable(self, var_name):
        '''
        para vars: a list of required variables
        type: list
        '''
        helper = {}
        helper['name'] = var_name
        helper['required'] = True
        self.env_variable[var_name] = self.variable(helper)

    def generate_task(self):
        '''
        generate task definition from template
        '''
        # read template file
        file_path = os.path.join(CLOUD_PIPE_TEMPLATES_FOLDER, 'ecs_task_definition_template.txt')
        with open(file_path, 'r') as tmpfile:
            template = json.load(tmpfile)

        template['family'] = self.name + name_generator.haikunate()

        template['containerDefinitions'][0]['memory'] = self.memory
        template['containerDefinitions'][0]['name'] = self.name + \
            name_generator.haikunate()
        template['containerDefinitions'][0]['image'] = self.image
        # TODO: change 32
        template['containerDefinitions'][0]['cpu'] = 32

        # add port
        for port in self.port.values():
            if port.host_port is not None:
                helper = {}
                helper['hostPort'] = port.host_port
                helper['containerPort'] = port.container_port
                helper['protocol'] = port.protocol
                template['containerDefinitions'][0]['portMappings'].append(helper)

        # print(json.dumps(template['containerDefinitions'][0]['portMappings']))

        if len(template['containerDefinitions'][0]['portMappings']) == 0:
            del template['containerDefinitions'][0]['portMappings']

        # add environment variable
        for var in self.env_variable.values():
            if var.value is not None:
                template['containerDefinitions'][0]['environment'].append(
                    {'name': var.name, 'value': var.value})

        if len(template['containerDefinitions'][0]['environment']) == 0:
            del template['containerDefinitions'][0]['environment']

        template['family'] = self.name + '-' + name_generator.haikunate()

        # print(json.dumps(template, sort_keys=True, indent='    '))
        return template
