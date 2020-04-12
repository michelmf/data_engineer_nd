import time
import random
import boto3
import json
import configparser as cfg
from botocore.exceptions import ClientError

class RedshiftCluster(object):
    
    ''' 
    Class that implements the IAC step and create a redshift cluster

    The created redshift cluster has these fixed paremeters:
        - Cluster Type    : multi-node
        - Number of Nodes : 2
        - Node Type       : dc2.large
    
    '''

    def __init__(self, iam_role_name, config_file='', cluster_name='project3', 
                 db_name='sparkify', db_user='sparkify_admin'):

        # IAM Role Name
        self.iam_role_name = iam_role_name
        self.role_arn      = ''
        
        # Redshift Cluster configuration
        self.cluster_type = 'multi-node'
        self.node_type    = 'dc2.large'
        self.num_nodes    = 2 
        self.cluster_name = cluster_name
        self.db_name      = db_name
        self.db_user      = db_user
        self.db_port      = 5439
        
        # Reading the configuration file
        try:
            config = cfg.ConfigParser()

            with(open(config_file,'r')) as filename:
                
                config.read_file(filename)

                self.iam = boto3.client('iam', aws_access_key_id=config.get('IAC','USER_KEY'),
                                     aws_secret_access_key=config.get('IAC','USER_SECRET'),
                                     region_name='sa-east-1') # My Region : Brazil

                self.redshift_client = boto3.client('redshift', aws_access_key_id=config.get('IAC','USER_KEY'),
                                    aws_secret_access_key=config.get('IAC','USER_SECRET'),
                                    region_name='sa-east-1') # My Region : Brazil

                self.ec2_client = boto3.resource('ec2', aws_access_key_id=config.get('IAC','USER_KEY'),
                                                 aws_secret_access_key=config.get('IAC','USER_SECRET'),
                                                 region_name='sa-east-1') # My Region : Brazil

            self.create_iam_role(iam_client=self.iam)
            self.attach_policy(iam_client=self.iam)

            # Random Password
            string = 'abcdABCDEFGHIJKLMNOPQRSTefghijklmnopqrstuvwxyz1234567890'
            db_password = ''.join([random.choice(string) for i in range(0,15)])

            self.create_redshift_cluster(ec2_client=self.ec2_client,
                                        redshift_client=self.redshift_client,
                                        db_name=self.db_name, 
                                        db_user=self.db_user,
                                        db_password=db_password,
                                        cluster_name=self.cluster_name)
            
            with(open(config_file,'w+')) as filename:
                
                # Writing the values on cfg file
                config.set('IAM_ROLE','ARN',f'{self.role_arn}')
                config.set('CLUSTER','db_host',f'{self.endpoint}')
                config.set('CLUSTER','db_name',f'{self.db_name}')
                config.set('CLUSTER','db_user',f'{self.db_user}')
                config.set('CLUSTER','db_password',f'{db_password}')
                config.write(filename)

        except Exception as e:
            print(f'Error while creating the class: {e}')
            
        print('Cluster configured.')

    def create_iam_role(self, iam_client):
        ''' 
        Create an IAM ROLE for redshift that allows it to call AWS services 
        '''

        try:
            print(f"Creating a new IAM Role named {self.iam_role_name}") 
            self.iam_role = iam_client.create_role(
                Path='/',
                RoleName=self.iam_role_name,
                Description = "Allows Redshift clusters to call AWS services on your behalf.",
                AssumeRolePolicyDocument=json.dumps(
                    {'Statement': [{'Action': 'sts:AssumeRole',
                    'Effect': 'Allow',
                    'Principal': {'Service': 'redshift.amazonaws.com'}}],
                    'Version': '2012-10-17'}))
            
            print(f'IAM ROLE {self.iam_role_name} CREATED')

        except Exception as e:
                print(f'Unable to create a new IAM role. Reason: {e}')
    
    def attach_policy(self, iam_client):
        '''
        This method attach the S3ReadOnlyAccess policy to the redshift role
        '''
        print('Attaching the S3ReadOnlyAccess policy to the redshift role')
        if (iam_client.attach_role_policy(RoleName=self.iam_role_name,
                                          PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                                          )['ResponseMetadata']['HTTPStatusCode'] != 200):

            raise Exception('Unable to attach role policy to Redshift Cluster')
        
        # Amazon Resource Name of IAM ROLE NAME
        self.role_arn = iam_client.get_role(RoleName=self.iam_role_name)['Role']['Arn']
   
    def create_redshift_cluster(self, ec2_client, redshift_client, db_name, db_user, db_password, cluster_name):

        try:
            
            response = redshift_client.create_cluster(        
            
                # Defined Hardware (Free Tier ...)
                ClusterType   = self.cluster_type,
                NodeType      = self.node_type,
                NumberOfNodes = self.num_nodes,

                # Identifiers & Credentials
                DBName             = self.db_name,
                ClusterIdentifier  = self.cluster_name,
                MasterUsername     = self.db_user,
                MasterUserPassword = db_password,
                
                #Roles (for s3 access)
                IamRoles=[self.role_arn]
                )

        except Exception as e:
            print(f'Unable to create a redshift cluster. Reason: {e}')
        
        # Waiting for the cluster being available 
        is_creating = True
        print('Setting up the cluster...')

        while (is_creating == True):

            time.sleep(10)
            status = redshift_client.describe_clusters(ClusterIdentifier=self.cluster_name)['Clusters'][0]['ClusterStatus']
            
            if (status == 'available'):
                is_creating = False

            print(f'Cluster Status: {status}')

        print('\n\nThe Redshift Cluster is Ready.')

        # Saving the created endpoint
        self.endpoint = redshift_client.describe_clusters(ClusterIdentifier=self.cluster_name)['Clusters'][0]['Endpoint']['Address']

        # Opening the ports
        print('Opening ports...')

        try:
            vpc = ec2_client.Vpc(id=redshift_client.describe_clusters(ClusterIdentifier=self.cluster_name)['Clusters'][0]['VpcId'])
            defaultSg = list(vpc.security_groups.all())[0]
            defaultSg.authorize_ingress(
                GroupName=defaultSg.group_name,
                CidrIp='0.0.0.0/0',
                IpProtocol='TCP',
                FromPort=int(self.db_port),
                ToPort=int(self.db_port)
            )

        except Exception as e:
            print(e)  

    def delete_cluster(self):
        pass

if __name__ == '__main__':

    print('Creating the cluster...\n\n')
    redshift = RedshiftCluster(config_file='../dwh.cfg', 
                               iam_role_name='test_redshift_cluster',
                               cluster_name='test', 
                               db_name='test', 
                               db_user='test')