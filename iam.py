import json
import logging


def attach_policy(iam_client, role_name, policy_arn):
    try:
        print('Attaching Policy....')
        response = iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn="arn:aws:iam::aws:policy/{}".format(policy_arn)
        )

        print('>> {}'.format(response))
        return True
    except Exception as e:
        logging.error(e)
        return False


def set_role_policy(iam_client, role_path='/', role_name='', role_description='', role_service='', policy_arns=[]):
    try:
        print('Creating new IAM role....')
        response_role = iam_client.create_role(
            Path=role_path,
            RoleName=role_name,
            Description=role_description,
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {'Service': role_service}}],
                 'Version': '2012-10-17'})
        )

        print('Attaching Policy....')
        for policy_arn in policy_arns:
            response_policy = iam_client.attach_role_policy(
                RoleName=role_name,
                PolicyArn="arn:aws:iam::aws:policy/{}".format(policy_arn)
            ).get('ResponseMetadata').get('HTTPStatusCode')
            # print('[{}][{}][{}]'.format(role_service, policy_arn, response_policy))

        print('Get the IAM role ARN....')
        role_arn = response_role.get('Role').get('Arn')

        print('>> ARN: {}'.format(role_arn))
        return True
    except Exception as e:
        logging.error(e)
        return False
