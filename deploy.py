import argparse
import json
import os
from pprint import pprint
from zipfile import ZipFile
import boto3
import uuid
from botocore.exceptions import ClientError

CLIENT = boto3.client('lambda')
CONFIGURATION_FILE = 'configurations.json'
API_FAILURE_LAMBDA_ARN = 'arn:aws:lambda:us-west-2:384060451980:function:apiGatewayFailure'
LAMBDA_FAILURE_LAMBDA_ARN = 'arn:aws:lambda:us-west-2:384060451980:function:LambdaFailure-6'
class LambdaStates:
    """This class contains constant states of a lambda function"""
    PENDING = 'Pending'
    IN_PROGRESS = 'InProgress'


class ApiGateway:
    """
    This class is reponsible for all the methods and actions that are required
    to deploy the api resource with a lambda function
    """

    CLIENT = boto3.client('apigateway')

    def __init__(self, api_gateway_id):
        self.api_gateway_id = api_gateway_id

    def create_api_resource(self, path_part):
        """This function will create the api resource with function name"""
        parent_id = self.get_parent_resource_id()
        self.CLIENT.create_resource(
            restApiId=self.api_gateway_id,
            parentId= parent_id,
            pathPart=path_part
        )

    def create_resource_method(self, resource_name, method_type):
        """This function will create resource method request"""
        response = self.get_api_resource()
        resource_id = None
        for resource in response['items']:
            if resource['path'] == ('/' + resource_name):
                resource_id = resource['id']

        self.CLIENT.put_method(
            restApiId=self.api_gateway_id,
            resourceId=resource_id,
            httpMethod=method_type,
            authorizationType='NONE'
        )

    def create_method_integration(self, resource_name, method_type):
        """This function will create resource integration request"""
        response = self.get_api_resource()
        
        resource_id = None
        for resource in response['items']:
            if resource['path'] == ('/' + resource_name):
                resource_id = resource['id']

        if method_type == 'OPTIONS':
            self.CLIENT.put_integration(
                restApiId=self.api_gateway_id,
                resourceId=resource_id,
                httpMethod='OPTIONS',
                type='MOCK',
                requestTemplates={
                    'application/json': '{"statusCode": 200}'
                }
            )
        else:
            lambda_uri = f"arn:aws:apigateway:us-west-2:lambda:path/2015-03-31/functions/arn:aws:lambda:us-west-2:384060451980:function:{resource_name}:${{stageVariables.lambdaAlias}}/invocations"
            self.CLIENT.put_integration(
                restApiId=self.api_gateway_id,
                resourceId=resource_id,
                httpMethod=method_type,
                type='AWS',
                integrationHttpMethod=method_type,
                uri=lambda_uri,
            )

    def create_method_response(self, resource_name, method_type):
        """This function will create resource method response"""
        response = self.get_api_resource()
        
        resource_id = None
        for resource in response['items']:
            if resource['path'] == ('/' + resource_name):
                resource_id = resource['id']

        if method_type == 'OPTIONS':
            self.CLIENT.put_method_response(
                restApiId=self.api_gateway_id,
                resourceId=resource_id,
                httpMethod=method_type,
                statusCode='200',
                responseParameters={
                    'method.response.header.Access-Control-Allow-Headers': False,
                    'method.response.header.Access-Control-Allow-Origin': False,
                    'method.response.header.Access-Control-Allow-Methods': False
                },
                responseModels={
                    'application/json': 'Empty'
                }
            )
        else:
            self.CLIENT.put_method_response(
                restApiId=self.api_gateway_id,
                resourceId=resource_id,
                httpMethod=method_type,
                statusCode='200',
                responseParameters={
                    'method.response.header.Access-Control-Allow-Origin': False
                },
                responseModels={
                    'application/json': 'Empty'
                }
            )

    def create_integration_response(self, resource_name, method_type):
        """This function will create resource integration request"""
        response = self.get_api_resource()
        
        resource_id = None
        for resource in response['items']:
            if resource['path'] == ('/' + resource_name):
                resource_id = resource['id']

        if method_type == 'OPTIONS':
            self.CLIENT.put_integration_response(
                restApiId=self.api_gateway_id,
                resourceId=resource_id,
                httpMethod=method_type,
                statusCode='200',
                responseParameters={
                    'method.response.header.Access-Control-Allow-Headers': '\'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token\'',
                    'method.response.header.Access-Control-Allow-Methods': '\'POST,OPTIONS\'',
                    'method.response.header.Access-Control-Allow-Origin': '\'*\''
                },
                responseTemplates={
                    'application/json': ''
                }
            )
        else:
            self.CLIENT.put_integration_response(
                restApiId=self.api_gateway_id,
                resourceId=resource_id,
                httpMethod=method_type,
                statusCode='200',
                responseParameters={
                    'method.response.header.Access-Control-Allow-Origin': '\'*\''
                },
                responseTemplates={
                    'application/json': ''
                }
            )

    def set_api_gateway_permissions(self, lambda_name, env):
        """This function is resposible for the permissions of api gateway"""
        client = boto3.client('lambda')
        client.add_permission(
            FunctionName= f"arn:aws:lambda:us-west-2:384060451980:function:{lambda_name}:{env}",
            SourceArn= f"arn:aws:execute-api:us-west-2:384060451980:{self.api_gateway_id}/*/POST/{lambda_name}",
            Principal= "apigateway.amazonaws.com",
            StatementId = "13e9d442-455f-4f17-9b73-616d9cbee339",
            Action= "lambda:InvokeFunction"
        )
        
    def get_parent_resource_id(self):
        """This function will get the resource parent id and return it"""
        parent_id = None
        response = self.get_api_resource()
        resources = response["items"]
        for item in resources:
            if item['path'] == '/':
                parent_id = item['id']
        return parent_id

    def check_api_existence(self, lambda_name):
        """this function will check if resource already exists"""
        response = self.get_api_resource()
        resources = response["items"]
        for item in resources:
            if 'pathPart' in item and item['pathPart'] == lambda_name:
                return True
        return False

    def get_api_resource(self):
        response = self.CLIENT.get_resources(
            restApiId= self.api_gateway_id,
            limit=500,
        )
        return response
    
    def deploy_api(self, stage_name):
        response = self.CLIENT.create_deployment(
                restApiId=self.api_gateway_id, 
                stageName=stage_name
        )

        return response

class LambdaCloudWatchLogs:
    """
    This class will have all the functions that will add a trigger to 
    LambdaLogger function, which will send an email in case of any failure in 
    the lambda
    """
    LOG_CLIENT = boto3.client('logs')
    
    def __init__(self, function_name):
        self.function_name = function_name
    
    def is_permission_added(self):
        function_name = '/aws/lambda/' + self.function_name
        try:
            res = CLIENT.get_policy(
                FunctionName= LAMBDA_FAILURE_LAMBDA_ARN
            )
        except ClientError as error:
            if (error.response['Error']['Code'] == 'ResourceNotFoundException'):
                return False
        policy = res['Policy']
        permissions = json.loads(policy)['Statement']
        for permission in permissions:
            sourceArn = permission['Condition']['ArnLike']['AWS:SourceArn']
            if function_name in sourceArn.split(':'):
                return True
        return False
    
    def add_permission(self):
        res = CLIENT.add_permission(
            FunctionName= LAMBDA_FAILURE_LAMBDA_ARN,
            StatementId= 'lambda-' + str(uuid.uuid4()),
            Action='lambda:InvokeFunction',
            Principal= 'logs.us-west-2.amazonaws.com',
            SourceArn= 'arn:aws:logs:us-west-2:384060451980:log-group:/aws/lambda/'
            + self.function_name +':*',
        )
        
    def add_subscription_filter(self, version_num):
        res = self.LOG_CLIENT.put_subscription_filter(
        logGroupName= '/aws/lambda/' + self.function_name,
        filterName= self.function_name + '-Log-Trigger',
        filterPattern= "?ERROR ?\"Task timed out\"",
        destinationArn= LAMBDA_FAILURE_LAMBDA_ARN,
        )
    
class APICloudWatchLogs:
    """
    This class will have all the functions that will add a trigger to 
    apiGatewayfailure function, which will send an email in case of any failure in 
    the api gateway
    """
    LAMBDA_CLIENT = boto3.client('lambda')
    LOG_CLIENT = boto3.client('logs')
    
    def __init__(self, apigateway_id):
        self.apigateway_id = apigateway_id
    
    def is_permission_added(self):
        try:
            res = CLIENT.get_policy(
                FunctionName= API_FAILURE_LAMBDA_ARN
            )
        except ClientError as error:
            if (error.response['Error']['Code'] == 'ResourceNotFoundException'):
                return False
        policy = res['Policy']
        permissions = json.loads(policy)['Statement']
        for permission in permissions:
            sourceArn = permission['Condition']['ArnLike']['AWS:SourceArn']
            if self.apigateway_id in sourceArn:
                return True
        return False
    
    def add_permission(self):
        CLIENT.add_permission(
            FunctionName= API_FAILURE_LAMBDA_ARN,
            StatementId= 'lambda-' + str(uuid.uuid4()),
            Action='lambda:InvokeFunction',
            Principal= 'logs.us-west-2.amazonaws.com',
            SourceArn= 'arn:aws:logs:us-west-2:384060451980:log-group:API-Gateway-Execution-Logs_'
             + self.apigateway_id + '/production:*'
        )
        
    def add_subscription_filter(self):
        res = self.LOG_CLIENT.put_subscription_filter(
        logGroupName= 'API-Gateway-Execution-Logs_'+ self.apigateway_id +'/production',
        filterName=  self.apigateway_id + '-Api-Trigger',
        filterPattern= "",
        destinationArn= API_FAILURE_LAMBDA_ARN,
        )
        
def display_lambda_data(lambda_data, func_name, env, description):
    """This function will display the details of a lambda function"""
    print("Lambda function details:\nFunction Name: " + func_name +
          "\nEnvironment: " + env + "\nDescription: " + description)
    pprint(lambda_data)
    print("---------------------------------------------------")

def retain_original_code(func_name):
    """
    As we change the code while deploying so we also need to revert it back on
    our local machine after we have deployed it on aws, this function will do
    that
    """
    file_obj = open(f"{func_name}-temp.py", "r", encoding='UTF-8')
    code_list = file_obj.readlines()

    with open(func_name + ".py", "w", encoding='UTF-8') as write_file:
        write_file.writelines(code_list)

    write_file.close()

def display_json_lambda_data(lambdas_data, env):
    """
    this function will display the data of all lambdas when we will
    use json file for the lambdas deployment
    """
    for data in lambdas_data:
        lambda_data = get_lambda_data(data.get('name'))
        display_lambda_data(lambda_data, data.get('name'), env,
                            data.get('description'))


def update_lambda_configuration(lambda_data):
    CLIENT.update_function_configuration(
        FunctionName=lambda_data.get('FunctionArn'),
        Handler= "lambda_function.lambda_handler",
        Runtime=lambda_data.get('Runtime'),
        Role=lambda_data.get('Role'),
        Timeout=lambda_data.get('Timeout'),
        MemorySize=lambda_data.get('MemorySize'),
    )

def change_env_in_code(function_name, env):
    """
    This function removes the env_constant import and change the env
    to the provided env
    """
    env_statement = f"ENVIRONMENT = '{env}'\n"
    file_obj = open(function_name + ".py", "r", encoding='UTF-8')
    code_list = file_obj.readlines()

    with open(function_name + "-temp.py", "w", encoding='UTF-8') as write_file:
        write_file.writelines(code_list)
    
    env_const_line = -1
    env_line = -1
    index = 0
    for line in code_list:
        if line.strip() == 'import environment':
            env_const_line = index
        if line.strip() == 'ENVIRONMENT = environment.ENVIRONMENT':
            env_line = index

        if env_const_line != -1 and env_line != -1:
            break
        index += 1

    if env_line != -1:
        code_list[env_line] = env_statement
    if env_const_line != -1:
        code_list.pop(env_const_line)

    with open(function_name + ".py", "w", encoding='UTF-8') as write_file:
        write_file.writelines(code_list)


def get_lambda_bundle_as_zip(function_name):
    """
    it will create a zip file of the lambda function and return that in
    bytes
    """
    create_lambda_bundle(function_name)
    with open(function_name + '.zip', 'rb') as file_data:
        lambda_code = file_data.read()
    return lambda_code

def get_lambda_data(func_name):
    """
    Using this function we can get the data of our lambda store in
    the configuration file
    """
    lambda_data = None
    file_data = open(CONFIGURATION_FILE, 'r', encoding='UTF-8')

    lambdas_data = json.load(file_data)
    if func_name in lambdas_data:
        lambda_data = lambdas_data.get(func_name)

    return lambda_data


def create_lambda_bundle(function_name):
    """this function will create the zip file of our lambda function"""
    with open(function_name + '.py') as file_obj:
        lines = file_obj.readlines()
    
    temp_file = open('lambda_function.py', 'w')
    temp_file.writelines(lines)
    temp_file.close()
    
    with ZipFile(function_name + '.zip', 'w') as zip_obj:
        zip_obj.write('lambda_function.py')

    os.remove('lambda_function.py')

def publish_code(function_name):
    """This function will publish our code on aws cloud"""
    create_lambda_bundle(function_name)
    
    bytes_content = get_lambda_bundle_as_zip(function_name)
    CLIENT.update_function_code(
        FunctionName=function_name,
        ZipFile=bytes_content
    )


def publish_lambda_version(function_name, description):
    response = CLIENT.publish_version(
        FunctionName=function_name,
        Description=description,
    )
    return response['Version']


def update_alias(function_name, env, version):
    """A function that will create alias according to the env"""

    # in this logic firstly we are checking if the alias already exists
    # if that is the case then we update the alias to newer version, else
    # we create a new alias for that version
    aliases_list = []
    all_aliases = CLIENT.list_aliases(
        FunctionName=function_name
    )['Aliases']
    for alias in all_aliases:
        aliases_list.append(alias['Name'])

    if env in aliases_list:
        CLIENT.update_alias(
            FunctionName=function_name,
            Name=env,
            FunctionVersion=version,
        )
    else:
        CLIENT.create_alias(
            FunctionName=function_name,
            Name=env,
            FunctionVersion=version,
        )

def deploy_api(function_name, env):
    file_object = open(CONFIGURATION_FILE, 'r', encoding='UTF-8')
    lambdas_data = json.load(file_object)
    if function_name in lambdas_data:
        api_id = lambdas_data[function_name]['ApiId']
        if api_id:
            print("Deploying API", api_id)
            api_object = ApiGateway(api_id)

            if not api_object.check_api_existence(function_name):
                api_object.create_api_resource(function_name)

                for method in ['POST', 'OPTIONS']:
                    api_object.create_resource_method(function_name, method)
                    api_object.create_method_integration(function_name, method)
                    api_object.create_method_response(function_name, method)
                    api_object.create_integration_response(function_name, method)

                try:
                    api_object.set_api_gateway_permissions(function_name, env)
                except ClientError:
                    pass
                res = api_object.deploy_api(env)
                print('API Deployed')
            else:
                print("Api resource already there")
                try:
                    api_object.set_api_gateway_permissions(function_name, env)
                except ClientError:
                    pass
                api_object.deploy_api(env)
        else:
            print('---------API Id is empty, please update it in ' +
                  'configuration file----------')

def add_lambda_log_trigger(function_name, version_num):
    file_object = open(CONFIGURATION_FILE, 'r', encoding='UTF-8')
    lambdas_data = json.load(file_object)
    if function_name in lambdas_data and 'logs' in lambdas_data[function_name]:
        status = lambdas_data[function_name]['logs']
        if status:
            obj = LambdaCloudWatchLogs(function_name)
            if not obj.is_permission_added():
                obj.add_permission()
                print("Permission Added on Lambda-Failure")
            else:
                print("Lambda Failure logs permission was already there")
            obj.add_subscription_filter(version_num)
            print("Subscription filter added in lambdaFailure")
            
def add_api_log_trigger(function_name):
    file_object = open(CONFIGURATION_FILE, 'r', encoding='UTF-8')
    lambdas_data = json.load(file_object)    
    if function_name in lambdas_data:
        api_id = lambdas_data[function_name]['ApiId']
        if api_id:
            obj = APICloudWatchLogs(api_id)
            if not obj.is_permission_added():
                obj.add_permission()
                print("Permission Added on Apigateway-Failure")
            else:
                print("APIGateway logs Permission was already there")
            obj.add_subscription_filter()
            print("Subscription filter added in Apigateway-Failure")

def clean_bundle(function_name):
    os.remove(function_name + ".zip")
    os.remove(function_name + "-temp.py")

def attach_layer(function_name):
    lambda_data = get_lambda_data(function_name)
    if lambda_data.get('layers'):
        CLIENT.update_function_configuration(
            FunctionName= function_name,
            Layers = lambda_data.get('layers')
        )
        print("Layers attached to the lambda function")
    else:
        print("Found no layers to attach, This will also remove any layer "
              + "already attached to the lambda function")

def update_wait(function_name):
    response = CLIENT.get_function_configuration(
        FunctionName=function_name,
    )
    while response['LastUpdateStatus'] == LambdaStates.IN_PROGRESS:
        response = CLIENT.get_function_configuration(
            FunctionName=function_name,
        )
        
def deploy_lambda(args):
    function_name = args[0]
    env = args[1]
    description = args[2]

    create_lambda_bundle(function_name)
    print('Lambda Bundle Created')

    update_wait(function_name)
    
    publish_code(function_name)
    print('Lambda Latest version updated with the new code')

    retain_original_code(function_name)

    update_wait(function_name)

    attach_layer(function_name)

    update_wait(function_name)

    version = publish_lambda_version(function_name, description)
    print(f'Lambda Version Created with id {version}')

    update_alias(function_name, env, version)
    print(f'Lambda Alias Updated to point to version {version}')
    
    deploy_api(function_name, env)
    
    if env == 'production':
        add_lambda_log_trigger(function_name, version)
        add_api_log_trigger(function_name)

    clean_bundle(function_name)
    print("---------------------------------------------------")


def validate_lambdas(lambdas_data):
    """
    The function that will validate the json file of lambdas that we want to
    deploy, It will check that if all the lambdas mentioned are also
    mentioned in configuration file
    """
    validate = True
    for data in lambdas_data:
        lambda_data = get_lambda_data(data.get('name'))
        if not lambda_data:
            print(f"*** Lambda {data.get('name')} does not exist in "
                  + "configuration file, please add it in file ***")
            validate = False
    return validate


def deploy_lambdas(args):
    """
    this function is responsible for calling necessary functions to deploy
    all those lambdas that are in the json file, in short this function will
    check all the sufficent info and then deploy/create those lambdas
    """
    filename = args.get("lambdas_file")
    file_obj = open(filename, 'r', encoding='UTF-8')
    lambdas_data = json.load(file_obj).get('functions')

    validate = validate_lambdas(lambdas_data)

    if validate:
        display_json_lambda_data(lambdas_data, args.get('env'))
        if get_lambda_confirmation():
            for data in lambdas_data:
                lambda_data = get_lambda_data(data.get('name'))
                func_data = [data.get('name'),
                             args.get('env'),
                             data.get('description')]

                print(func_data[0])
                if not check_lambda_existence(func_data[0]):
                    create_lambda_on_aws(lambda_data, func_data[0])
                    print("Lambda created on AWS")
                else:
                    update_lambda_configuration(lambda_data)
                response = CLIENT.get_function_configuration(
                        FunctionName=func_data[0],
                    )
                while response['State'] == LambdaStates.PENDING:
                    response = CLIENT.get_function_configuration(
                        FunctionName=func_data[0],
                    )
                change_env_in_code(func_data[0], func_data[1])
                deploy_lambda(func_data)

def check_lambda_existence(lambda_name):
    """Checking if wether the given lambda exists on aws cloud"""
    try:
        CLIENT.get_function(
            FunctionName=lambda_name,
        )
    except ClientError as error:
        if (error.response['Error']['Code'] ==
                'ResourceNotFoundException'):
            return False
    return True


def create_lambda_on_aws(lambda_data, func_name):
    CLIENT.create_function(
        FunctionName=lambda_data.get('FunctionArn'),
        Handler= "lambda_function.lambda_handler",
        Runtime=lambda_data.get('Runtime'),
        Role=lambda_data.get('Role'),
        Timeout=lambda_data.get('Timeout'),
        MemorySize=lambda_data.get('MemorySize'),
        Code={
            'ZipFile': get_lambda_bundle_as_zip(func_name)
        },
        PackageType='Zip',
        Layers= lambda_data.get('layers')
    )


def get_lambda_confirmation():
    """Helping function just to get yes or no from user"""
    choice = input("Are you sure you want to create/deploy this lambda" +
                   "(Y/N): ")
    return choice in ('y', 'Y')


def deploy_single_lambda(args, lambda_data):
    """
    function responsible to create/deploy a single lambda on aws
    """
    func_data = [vars(args).get('lambda'),
                 vars(args).get('env'),
                 vars(args).get('description')]
    display_lambda_data(lambda_data, func_data[0], func_data[1], func_data[2])
    if get_lambda_confirmation():
        if not check_lambda_existence(func_data[0]):
            create_lambda_on_aws(lambda_data, func_data[0])
            print("Lambda created on AWS")
        else:
            update_lambda_configuration(lambda_data)
        response = CLIENT.get_function_configuration(
                FunctionName=func_data[0],
            )
        while response['State'] == LambdaStates.PENDING:
            response = CLIENT.get_function_configuration(
                FunctionName=func_data[0],
            )
        change_env_in_code(func_data[0], func_data[1])
        deploy_lambda(func_data)


def validate_arguments():
    parser = argparse.ArgumentParser(allow_abbrev=False)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-lambda')
    group.add_argument('-lambdas_file')
    parser.add_argument('-env', type=str, required=True,
                        help="The environment of lambda")
    parser.add_argument('-description', type=str, help="Description of lambda")
    args = parser.parse_args()
    return args


def main():
    args = validate_arguments()
    if vars(args).get('lambda') and not vars(args).get('description'):
        print("usage: deploy.py [-h] (-lambda LAMBDA) -env ENV "
              + "-description DESCRIPTION")
        print("deploy.py: error: the following arguments are required: -description")
    elif vars(args).get('lambda') is not None:
        lambda_data = get_lambda_data(vars(args).get('lambda'))
        if lambda_data:
            deploy_single_lambda(args, lambda_data)
        elif not lambda_data:
            print("**** The lambda function is not found in configuration"
                  + " file, Please add it in the file ****")
    elif vars(args).get('lambdas_file') is not None:
        deploy_lambdas(vars(args))


if __name__ == "__main__":
    main()