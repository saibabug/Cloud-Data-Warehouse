import configparser
import boto3
import json
from create_resources import get_config

def shutdown_resources():
     """shutdowns resources created
    Args:
        cur (cursor): cursor to execute queries
        conn: open connection 
    """

    # parse config file
    config = get_config('dwh.cfg')

    # create resources/clients
    iam = boto3.client('iam', region_name=config['REDSHIFT']['REGION_NAME'],aws_access_key_id=config['AWS']['key'], aws_secret_access_key=config['AWS']['secret'])
    redshift = boto3.client('redshift', region_name=config['REDSHIFT']['REGION_NAME'],aws_access_key_id=config['AWS']['key'], aws_secret_access_key=config['AWS']['secret'])
    ec2 = boto3.resource('ec2', region_name=config['REDSHIFT']['REGION_NAME'],aws_access_key_id=config['AWS']['key'], aws_secret_access_key=config['AWS']['secret'])

    try:
        # delete cluster
        print('Deleting Redshift cluster {}. This might take a few minutes ...'.format(config['REDSHIFT']['CLUSTER_IDENTIFIER']))
        response = redshift.delete_cluster( ClusterIdentifier=config['REDSHIFT']['cluster_identifier'],  
                                            SkipFinalClusterSnapshot=True)
        # Wait for up to 30 minutes until the cluster is deleted successfully
        redshift.get_waiter('cluster_deleted').wait(ClusterIdentifier=config['REDSHIFT']['cluster_identifier'],
                                                    WaiterConfig={'Delay': 30, 'MaxAttempts': 60})

        # delete role and attached policy
        print('Deleting IAM Role {}'.format(config['REDSHIFT']['ROLE_NAME']))
        iam.detach_role_policy(RoleName=config['REDSHIFT']['ROLE_NAME'],
                               PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        iam.delete_role(RoleName=config['REDSHIFT']['ROLE_NAME'])

        # revoke ingress rules
        sg = list(ec2.security_groups.all())[0]
        print('Revoking Ingress rules for SecurityGroup {}'.format(sg))
        sg.revoke_ingress(GroupName=sg.group_name,
                          CidrIp='0.0.0.0/0',
                          IpProtocol='tcp',
                          FromPort=int(config['REDSHIFT']['DB_PORT']),
                          ToPort=int(config['REDSHIFT']['DB_PORT']))
    except Exception as e:
        print(e)

    print('Clean up completed. All resources deleted.')

if __name__ == "__main__":
    shutdown_resources()