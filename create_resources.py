import configparser
import pandas as pd
import boto3
import json

def get_config(filename):
    """returns configuration object which can be used for creating AWS resources
    
    Args: 
    filename: config file name
    
    return config object to fetch values from configuration file
    """
    config = configparser.ConfigParser()
    try:
        config.read_file(open('dwh.cfg'))
    except Exception as e:
        print(e)

    return config

def update_config_file(config_file, section, key, value):
    """Writes to an existing config file
    Args:
        config_file (ConfigParser object): Configuration file the user wants to update
        section (string): The section on the config file the user wants to write
        key (string): The key the user wants to write
        value (string): The value the user wants to write
    """
    try:
        # Reading cfg file
        config = configparser.ConfigParser()
        config.read(config_file)

        #Setting  Section, Key and Value to be write on the cfg file
        config.set(section, key, value)

        # Writing to cfg file
        with open(config_file, 'w') as f:
            config.write(f)  
    except ClientError as e:
        print(f'ERROR: {e}')
        
def create_iam_role(iam, config):
    """
    Create an IAM role to allow Redshift to access S3.
    Arg(s):
        iam: IAM resource/client
        config: an object that contains necessary information for setting up the cluster
    Return(s):
        AWS RoleARN
    """

    try:
        print('Creating IAM Role')
        dwhRole = iam.create_role(
            RoleName=config['REDSHIFT']['ROLE_NAME'],
            Description='IAM Role to allow Redshift to access S3 bucket ReadOnly',
            AssumeRolePolicyDocument=json.dumps(
                {'Version': '2012-10-17',
                'Statement':
                [{'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}
                }]}))
        # Wait for up to 30 seconds until the role is created successfully
        iam.get_waiter('role_exists').wait(
            RoleName=config['REDSHIFT']['ROLE_NAME'],
            WaiterConfig={'Delay': 1, 'MaxAttempts': 30})
    except Exception as e:
        print(e)

    try:
        print('Attaching Policy')
        response = iam.attach_role_policy(
            RoleName=config['REDSHIFT']['ROLE_NAME'], 
            PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess')
    except Exception as e:
        print(e)

    try:
        roleArn = iam.get_role(RoleName=config['REDSHIFT']['ROLE_NAME'])['Role']['Arn']
    except Exception as e:
        print(e)

    return roleArn


def create_redshift_cluster(redshift, config, roleArn):
    """
    Create a Redshfit cluster.
    Arg(s):
        redshift: a redshift resource/client
        config: an object that contains necessary information for setting up the cluster
        roleArn: created to be attached to the cluster
    Return(s):
        cluster_prop: a cluster descriptor
    """

    try:
        print('Creating a Redshift Cluster. This might take a few minutes ...')
        response = redshift.create_cluster(
            # parameters for hardware
            ClusterType=config['REDSHIFT']['CLUSTER_TYPE'],
            NodeType=config['REDSHIFT']['NODE_TYPE'],
            NumberOfNodes=int(config['REDSHIFT']['NUM_NODES']),
            # parameters for identifiers & credentials
            ClusterIdentifier=config['REDSHIFT']['CLUSTER_IDENTIFIER'],
            DBName=config['REDSHIFT']['DB_NAME'],
            MasterUsername=config['REDSHIFT']['DB_MASTER_USER'],
            MasterUserPassword=config['REDSHIFT']['DB_MASTER_PASSWORD'],
            Port=int(config['REDSHIFT']['DB_PORT']),
            # parameter for role (to allow s3 access)
            IamRoles=[roleArn])
        # Wait for up to 30 minutes until the cluster is created successfully
        redshift.get_waiter('cluster_available').wait(
            ClusterIdentifier=config['REDSHIFT']['CLUSTER_IDENTIFIER'],
            WaiterConfig={'Delay': 30, 'MaxAttempts': 60})
    except Exception as e:
        print(e)

    # describe the cluster (codes borrowed from Lesson 3 Exercise 2)
    try:
        cluster_props = redshift.describe_clusters(ClusterIdentifier=config['REDSHIFT']['CLUSTER_IDENTIFIER'])['Clusters'][0]
    except Exception as e:
        print(e)

    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in cluster_props.items() if k in keysToShow]
    df = pd.DataFrame(data=x, columns=["Key", "Value"])
    print(df)

    return cluster_props


def create_ec2_sg(ec2, config, cluster_props):
    """
    Create a security group for Redshift cluster
    Arg(s):
        ec2: an EC2 resource/client
        config: an object that contains necessary information for setting up the cluster
        cluster_props: an dict that describes the cluster
    Return(s):
        sg: a default security group
    """

    try:
        vpc = ec2.Vpc(id=cluster_props['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]

        defaultSg.authorize_ingress(
            GroupName= defaultSg.group_name,
            CidrIp='0.0.0.0/0', # allow traffic from any IP source
            IpProtocol='tcp',
            FromPort=int(config['REDSHIFT']['DB_PORT']),
            ToPort=int(config['REDSHIFT']['DB_PORT'])
            )
    except Exception as e:
        print(e)

    return defaultSg
    
def create_resources():
    config = get_config('dwh.cfg')
    # create resources/clients
    iam = boto3.client('iam', region_name=config['REDSHIFT']['REGION_NAME'], aws_access_key_id=config['AWS']['key'], aws_secret_access_key=config['AWS']['secret'])
    redshift = boto3.client('redshift', region_name=config['REDSHIFT']['REGION_NAME'], aws_access_key_id=config['AWS']['key'], aws_secret_access_key=config['AWS']['secret'])
    ec2 = boto3.resource('ec2', region_name=config['REDSHIFT']['REGION_NAME'], aws_access_key_id=config['AWS']['key'], aws_secret_access_key=config['AWS']['secret'])

    roleArn = create_iam_role(iam, config)
    cluster_props = create_redshift_cluster(redshift, config, roleArn)
    sg = create_ec2_sg(ec2, config, cluster_props)
    update_config_file('dwh.cfg', 'REDSHIFT', 'HOST', cluster_props['Endpoint']['Address'])
    update_config_file('dwh.cfg', 'IAM_ROLE', 'ARN', roleArn)

    print('Cluster Setup done.')
    print('RoleArn: {}'.format(roleArn))
    print('Cluster Endpoint: {}'.format(cluster_props['Endpoint']['Address']))
    print('SecurityGroup: {}'.format(sg))
    print("SecurityGroup's Name: {}".format(sg.group_name))
    print("SecurityGroup's IP permissions: {}".format(sg.ip_permissions))
    
if __name__ == "__main__":
    create_resources()
    

