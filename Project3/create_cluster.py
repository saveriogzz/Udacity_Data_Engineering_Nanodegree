import boto3
import json
import configparser
import time

from botocore.exceptions import ClientError


def create_iam_role(iam, DWH_IAM_ROLE_NAME):
    """
    Creates IAM Role for Redshift
    """

    try:
        print("1.1 Creating a new IAM Role")
        dwhRole = iam.create_role(
                            Path='/',
                            RoleName=DWH_IAM_ROLE_NAME,
                            Description='Allows Redshift clusters to call AWS services on your behalf.',
                            AssumeRolePolicyDocument=json.dumps({'Statement':[{'Action':'sts:AssumeRole',
                                                                'Effect':'Allow',
                                                                'Principal':{'Service':'redshift.amazonaws.com'}}],
                                                                'Version':'2012-10-17'})
                                )
        
        print("="*40)              
        print("1.2 Attaching Policy")
        iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                            )['ResponseMetadata']['HTTPStatusCode']

        print("="*40)
        print("1.3 Get the IAM role ARN")
        roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

        print("    IAM_RoleArn: {}".format(roleArn))

        print("="*40)
        return roleArn

    except Exception as e:
        print("+"*40)
        print(e)
        print("+"*40)
 


def create_clust(redshift, roleArn, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, 
                  DWH_NUM_NODES, DWH_DB, DWH_CLUSTER_IDENTIFIER,
                  DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT):
    """
    Create Redshift cluster.
    """

    try:
        response = redshift.create_cluster(        
                                # hardware
                                ClusterType = DWH_CLUSTER_TYPE,
                                NodeType = DWH_NODE_TYPE,
                                NumberOfNodes = int(DWH_NUM_NODES),

                                # identifiers & credentials
                                DBName = DWH_DB,
                                ClusterIdentifier = DWH_CLUSTER_IDENTIFIER,
                                MasterUsername = DWH_DB_USER,
                                MasterUserPassword = DWH_DB_PASSWORD,
                                
                                Port = int(DWH_PORT),
                                
                                # role (to allow s3 access)
                                IamRoles = [roleArn]
                            )
        
    except Exception as e:
        print("+"*40)
        print(e)
        print("+"*40)
    

def get_clust_props(redshift, DWH_CLUSTER_IDENTIFIER):
    """
    Retrieve Redshift clusters properties.
    """
    try:
        cluster_props = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        DWH_ENDPOINT = cluster_props['Endpoint']['Address']
        DWH_ROLE_ARN = cluster_props['IamRoles'][0]['IamRoleArn']

        print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
        print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)
    
        return cluster_props

    except:
        print("+"*40)
        print(e)
        print("+"*40)


def open_TCP_port(cluster_props):
    """
    Update clusters security group to allow access through redshift port.
    """

    try:
        vpc = ec2.Vpc(id=cluster_props['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
                    GroupName=defaultSg.group_name,
                    CidrIp='0.0.0.0/0',
                    IpProtocol='TCP',
                    FromPort=int(DWH_PORT),
                    ToPort=int(DWH_PORT)
                )
    except Exception as e:
        print("+"*40)
        print(e)
        print("+"*40)


def main():
    # defining variables in the global scope

    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')

    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")

    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")



    iam = boto3.client('iam',
                  region_name="us-west-2",
                  aws_access_key_id=KEY,
                  aws_secret_access_key=SECRET
                  )

    redshift = boto3.client('redshift',
                  region_name="us-west-2",
                  aws_access_key_id=KEY,
                  aws_secret_access_key=SECRET
                  )

    ec2 = boto3.resource('ec2',
                  region_name="us-west-2",
                  aws_access_key_id=KEY,
                  aws_secret_access_key=SECRET
                  )
    
    # s3 = boto3.resource('s3',
    #                     region_name="us-west-2",
    #                     aws_access_key_id=KEY,
    #                     aws_secret_access_key=SECRET
    #                 )

    
    # calling functions
    roleArn = create_iam_role(iam, DWH_IAM_ROLE_NAME)

    create_clust(redshift, roleArn, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, 
                  DWH_NUM_NODES, DWH_DB, DWH_CLUSTER_IDENTIFIER,
                  DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT)

    while redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]['ClusterStatus'] != 'available':
        time.sleep(5)

    cluster_props = get_clust_props(redshift, DWH_CLUSTER_IDENTIFIER)


    open_TCP_port(ec2, cluster_props, DWH_PORT)

    

if __name__ == "__main__":
    main()