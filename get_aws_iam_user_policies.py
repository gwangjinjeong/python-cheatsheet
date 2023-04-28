import json
import boto3

iam_client = boto3.client("iam")

def get_group(username : str) -> list:
    lst = []
    response = iam_client.list_groups_for_user(
        UserName = username)
    if response['Groups']:
        for group in response['Groups']:
            lst.append({
                'GroupName':group['GroupName'], 
                'GroupArn':group['Arn']})
        return lst
    else:
        return []
        
def get_users_managed_policy(username : str)-> list:
    lst = []
    response = iam_client.list_attached_user_policies(
        UserName=username)
    if response['AttachedPolicies']:
        for policy in response['AttachedPolicies']:
          latest_version = iam_client.list_policy_versions(PolicyArn=policy['PolicyArn'])['Versions'][0]['VersionId']
          policyjson = iam_client.get_policy_version(
              PolicyArn=policy['PolicyArn'],
              VersionId=latest_version)
          lst.append({ 
                'PolicyName' : policy['PolicyName'],
                'PolicyArn' : policy['PolicyArn'],
                'PolicyJSON' : policyjson['PolicyVersion']['Document']})
        return lst
    else:
        return []
    return lst

def get_users_inline_policy(username : str) -> list:
    lst = []
    response = iam_client.list_user_policies(
        UserName=username)
    if response['PolicyNames']:
        for policy in response['PolicyNames']:
            policyjson = iam_client.get_user_policy(
                UserName=username,
                PolicyName=policy)  
            lst.append({
                'PolicyName' : policy,
                'PolicyJSON' : policyjson['PolicyDocument']
            })
        return lst
    else:
        return []
    return lst
    
def list_users():
    result = {'UserDetailList' : []}
    paginator = iam_client.get_paginator('list_users')
    for response in paginator.paginate():
        for user in response["Users"]:
            result['UserDetailList'].append({
                'userName' : user['UserName'],
                'userArn' : user['Arn'],
                'groupName' : get_group(user['UserName']),
                'policy' : get_users_inline_policy(user['UserName']) + get_users_managed_policy(user['UserName']),
            })
    return result

def lambda_handler(event, context):
    result = list_users()
    print(result)
    return {
        'statusCode': 200,
        'body': json.dumps(result)
    }
