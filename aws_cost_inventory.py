import boto3
import json
from datetime import datetime, timedelta
from botocore.exceptions import ClientError, NoCredentialsError
import sys

class AWSCostInventory:
    def __init__(self):
        self.inventory = {}
        self.errors = []
        
    def log_error(self, service, region, error):
        """Log de erros para análise posterior"""
        self.errors.append({
            "service": service,
            "region": region,
            "error": str(error),
            "timestamp": datetime.utcnow().isoformat()
        })
    
    def get_all_regions(self):
        """Obter todas as regiões AWS disponíveis"""
        try:
            ec2 = boto3.client('ec2', region_name='us-east-1')
            regions = [r['RegionName'] for r in ec2.describe_regions()['Regions']]
            return regions
        except Exception as e:
            self.log_error('ec2', 'global', e)
            return ['us-east-1', 'us-west-2', 'eu-west-1']  # fallback básico
    
    def list_ec2_instances(self):
        """Listar instâncias EC2 com informações de storage"""
        instances = []
        regions = self.get_all_regions()
        
        for region in regions:
            try:
                ec2 = boto3.client('ec2', region_name=region)
                response = ec2.describe_instances()
                
                for reservation in response['Reservations']:
                    for instance in reservation['Instances']:
                        # Obter tags para identificação
                        tags = {tag['Key']: tag['Value'] for tag in instance.get('Tags', [])}
                        
                        # Obter volumes anexados
                        volumes = []
                        if 'BlockDeviceMappings' in instance:
                            for mapping in instance['BlockDeviceMappings']:
                                volume_id = mapping.get('Ebs', {}).get('VolumeId')
                                if volume_id:
                                    try:
                                        volume_info = ec2.describe_volumes(VolumeIds=[volume_id])
                                        volume = volume_info['Volumes'][0]
                                        volumes.append({
                                            'VolumeId': volume_id,
                                            'Size': volume['Size'],
                                            'VolumeType': volume['VolumeType'],
                                            'Encrypted': volume['Encrypted']
                                        })
                                    except Exception as e:
                                        self.log_error('ec2-volume', region, e)
                        
                        instances.append({
                            'Region': region,
                            'InstanceId': instance['InstanceId'],
                            'InstanceType': instance['InstanceType'],
                            'State': instance['State']['Name'],
                            'LaunchTime': instance.get('LaunchTime', '').isoformat() if instance.get('LaunchTime') else None,
                            'Platform': instance.get('Platform', 'Linux'),
                            'VPC': instance.get('VpcId'),
                            'SubnetId': instance.get('SubnetId'),
                            'PublicIP': instance.get('PublicIpAddress'),
                            'PrivateIP': instance.get('PrivateIpAddress'),
                            'Name': tags.get('Name', 'N/A'),
                            'Environment': tags.get('Environment', 'N/A'),
                            'Volumes': volumes
                        })
            except Exception as e:
                self.log_error('ec2', region, e)
        
        return instances
    
    def list_ebs_volumes(self):
        """Listar volumes EBS"""
        volumes = []
        regions = self.get_all_regions()
        
        for region in regions:
            try:
                ec2 = boto3.client('ec2', region_name=region)
                response = ec2.describe_volumes()
                
                for volume in response['Volumes']:
                    tags = {tag['Key']: tag['Value'] for tag in volume.get('Tags', [])}
                    
                    volumes.append({
                        'Region': region,
                        'VolumeId': volume['VolumeId'],
                        'Size': volume['Size'],
                        'VolumeType': volume['VolumeType'],
                        'State': volume['State'],
                        'AvailabilityZone': volume['AvailabilityZone'],
                        'Encrypted': volume['Encrypted'],
                        'AttachedInstance': volume['Attachments'][0]['InstanceId'] if volume['Attachments'] else None,
                        'IOPS': volume.get('Iops', 0),
                        'Name': tags.get('Name', 'N/A')
                    })
            except Exception as e:
                self.log_error('ebs', region, e)
        
        return volumes
    
    def list_s3_buckets(self):
        """Listar buckets S3 com tamanhos usando múltiplas estratégias"""
        buckets = []
        try:
            s3 = boto3.client('s3')
            response = s3.list_buckets()
            
            for bucket in response['Buckets']:
                bucket_name = bucket['Name']
                print(f"🔍 Analisando bucket: {bucket_name}")
                
                # Obter região do bucket
                try:
                    bucket_region = s3.get_bucket_location(Bucket=bucket_name)['LocationConstraint']
                    if bucket_region is None:
                        bucket_region = 'us-east-1'
                except:
                    bucket_region = 'unknown'
                
                # Múltiplas estratégias para obter o tamanho
                size_info = self.get_bucket_size(bucket_name, bucket_region)
                
                buckets.append({
                    'BucketName': bucket_name,
                    'Region': bucket_region,
                    'CreationDate': bucket['CreationDate'].isoformat(),
                    'Size': size_info['formatted_size'],
                    'ObjectCount': size_info['object_count'],
                    'SizeBytes': size_info['size_bytes']
                })
                
        except Exception as e:
            self.log_error('s3', 'global', e)
        
        return buckets

    def get_bucket_size(self, bucket_name, bucket_region):
        """Obter tamanho do bucket usando várias estratégias"""
        size_info = {
            'size_bytes': 0,
            'object_count': 0,
            'formatted_size': 'N/A'
        }
        
        try:
            s3 = boto3.client('s3')
            
            # Estratégia 1: Listar objetos diretamente (mais preciso mas pode ser lento)
            print(f"   📋 Listando objetos do bucket {bucket_name}...")
            paginator = s3.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=bucket_name)
            
            total_size = 0
            total_count = 0
            
            for page in page_iterator:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        total_size += obj['Size']
                        total_count += 1
            
            size_info['size_bytes'] = total_size
            size_info['object_count'] = total_count
            
            # Formatar tamanho
            if total_size == 0:
                size_info['formatted_size'] = "0 B (Bucket vazio)"
            elif total_size < 1024:
                size_info['formatted_size'] = f"{total_size} B"
            elif total_size < 1024**2:
                size_info['formatted_size'] = f"{total_size/1024:.2f} KB"
            elif total_size < 1024**3:
                size_info['formatted_size'] = f"{total_size/(1024**2):.2f} MB"
            elif total_size < 1024**4:
                size_info['formatted_size'] = f"{total_size/(1024**3):.2f} GB"
            else:
                size_info['formatted_size'] = f"{total_size/(1024**4):.2f} TB"
            
            print(f"   ✅ Bucket {bucket_name}: {size_info['formatted_size']} ({total_count} objetos)")
            return size_info
            
        except Exception as e:
            print(f"   ⚠️ Erro ao listar objetos diretamente: {str(e)}")
            # Fallback: Tentar CloudWatch se a listagem direta falhar
            try:
                print(f"   🔄 Tentando CloudWatch para {bucket_name}...")
                cw_region = 'us-east-1' if bucket_region == 'us-east-1' else bucket_region
                cw = boto3.client('cloudwatch', region_name=cw_region)
                
                # Tentar obter métricas dos últimos 7 dias
                metric = cw.get_metric_statistics(
                    Namespace='AWS/S3',
                    MetricName='BucketSizeBytes',
                    Dimensions=[
                        {'Name': 'BucketName', 'Value': bucket_name},
                        {'Name': 'StorageType', 'Value': 'StandardStorage'}
                    ],
                    StartTime=datetime.utcnow() - timedelta(days=7),
                    EndTime=datetime.utcnow(),
                    Period=86400,
                    Statistics=['Average']
                )
                
                if metric['Datapoints']:
                    size_bytes = metric['Datapoints'][-1]['Average']
                    size_info['size_bytes'] = size_bytes
                    
                    if size_bytes < 1024:
                        size_info['formatted_size'] = f"{size_bytes:.0f} B"
                    elif size_bytes < 1024**2:
                        size_info['formatted_size'] = f"{size_bytes/1024:.2f} KB"
                    elif size_bytes < 1024**3:
                        size_info['formatted_size'] = f"{size_bytes/(1024**2):.2f} MB"
                    else:
                        size_info['formatted_size'] = f"{size_bytes/(1024**3):.2f} GB"
                
                # Tentar obter contagem de objetos
                count_metric = cw.get_metric_statistics(
                    Namespace='AWS/S3',
                    MetricName='NumberOfObjects',
                    Dimensions=[
                        {'Name': 'BucketName', 'Value': bucket_name},
                        {'Name': 'StorageType', 'Value': 'AllStorageTypes'}
                    ],
                    StartTime=datetime.utcnow() - timedelta(days=7),
                    EndTime=datetime.utcnow(),
                    Period=86400,
                    Statistics=['Average']
                )
                
                if count_metric['Datapoints']:
                    size_info['object_count'] = int(count_metric['Datapoints'][-1]['Average'])
                
                print(f"   ✅ CloudWatch {bucket_name}: {size_info['formatted_size']}")
                    
            except Exception as cw_error:
                print(f"   ❌ Erro no CloudWatch: {str(cw_error)}")
                self.log_error('cloudwatch-s3', bucket_region, cw_error)
                size_info['formatted_size'] = "Erro ao obter tamanho"
            
            self.log_error('s3-bucket-size', bucket_name, e)
        
        return size_info
    
    def list_rds_instances(self):
        """Listar instâncias RDS"""
        rds_instances = []
        regions = self.get_all_regions()
        
        for region in regions:
            try:
                rds = boto3.client('rds', region_name=region)
                
                # RDS Instances
                instances = rds.describe_db_instances()
                for instance in instances['DBInstances']:
                    rds_instances.append({
                        'Region': region,
                        'DBInstanceIdentifier': instance['DBInstanceIdentifier'],
                        'DBInstanceClass': instance['DBInstanceClass'],
                        'Engine': instance['Engine'],
                        'EngineVersion': instance['EngineVersion'],
                        'DBInstanceStatus': instance['DBInstanceStatus'],
                        'AllocatedStorage': instance['AllocatedStorage'],
                        'StorageType': instance.get('StorageType', 'N/A'),
                        'MultiAZ': instance['MultiAZ'],
                        'PubliclyAccessible': instance['PubliclyAccessible'],
                        'AvailabilityZone': instance.get('AvailabilityZone', 'N/A'),
                        'VpcId': instance.get('DbSubnetGroup', {}).get('VpcId', 'N/A')
                    })
                    
                # RDS Clusters (Aurora)
                try:
                    clusters = rds.describe_db_clusters()
                    for cluster in clusters['DBClusters']:
                        rds_instances.append({
                            'Region': region,
                            'DBClusterIdentifier': cluster['DBClusterIdentifier'],
                            'Engine': cluster['Engine'],
                            'EngineVersion': cluster['EngineVersion'],
                            'Status': cluster['Status'],
                            'DatabaseName': cluster.get('DatabaseName', 'N/A'),
                            'MasterUsername': cluster['MasterUsername'],
                            'ClusterMembers': len(cluster.get('DBClusterMembers', [])),
                            'Type': 'Aurora Cluster'
                        })
                except:
                    pass  # Região pode não suportar Aurora
                    
            except Exception as e:
                self.log_error('rds', region, e)
        
        return rds_instances
    
    def list_load_balancers(self):
        """Listar Load Balancers (ELB, ALB, NLB)"""
        load_balancers = []
        regions = self.get_all_regions()
        
        for region in regions:
            try:
                # ELBv2 (ALB/NLB)
                elbv2 = boto3.client('elbv2', region_name=region)
                lbs = elbv2.describe_load_balancers()
                
                for lb in lbs['LoadBalancers']:
                    load_balancers.append({
                        'Region': region,
                        'LoadBalancerName': lb['LoadBalancerName'],
                        'LoadBalancerArn': lb['LoadBalancerArn'],
                        'Type': lb['Type'],
                        'Scheme': lb['Scheme'],
                        'State': lb['State']['Code'],
                        'VpcId': lb.get('VpcId', 'N/A'),
                        'AvailabilityZones': [az['ZoneName'] for az in lb.get('AvailabilityZones', [])],
                        'CreatedTime': lb['CreatedTime'].isoformat()
                    })
                
                # Classic Load Balancers
                elb = boto3.client('elb', region_name=region)
                classic_lbs = elb.describe_load_balancers()
                
                for clb in classic_lbs['LoadBalancerDescriptions']:
                    load_balancers.append({
                        'Region': region,
                        'LoadBalancerName': clb['LoadBalancerName'],
                        'Type': 'classic',
                        'Scheme': clb['Scheme'],
                        'VpcId': clb.get('VPCId', 'Classic'),
                        'AvailabilityZones': clb['AvailabilityZones'],
                        'CreatedTime': clb['CreatedTime'].isoformat()
                    })
                    
            except Exception as e:
                self.log_error('elb', region, e)
        
        return load_balancers
    
    def list_lambda_functions(self):
        """Listar funções Lambda"""
        functions = []
        regions = self.get_all_regions()
        
        for region in regions:
            try:
                lambda_client = boto3.client('lambda', region_name=region)
                response = lambda_client.list_functions()
                
                for func in response['Functions']:
                    functions.append({
                        'Region': region,
                        'FunctionName': func['FunctionName'],
                        'Runtime': func.get('Runtime', 'N/A'),
                        'CodeSize': func['CodeSize'],
                        'MemorySize': func['MemorySize'],
                        'Timeout': func['Timeout'],
                        'LastModified': func['LastModified'],
                        'State': func.get('State', 'N/A'),
                        'FunctionArn': func['FunctionArn']
                    })
                    
            except Exception as e:
                self.log_error('lambda', region, e)
        
        return functions
    
    def list_nat_gateways(self):
        """Listar NAT Gateways"""
        nat_gateways = []
        regions = self.get_all_regions()
        
        for region in regions:
            try:
                ec2 = boto3.client('ec2', region_name=region)
                response = ec2.describe_nat_gateways()
                
                for nat in response['NatGateways']:
                    tags = {tag['Key']: tag['Value'] for tag in nat.get('Tags', [])}
                    
                    nat_gateways.append({
                        'Region': region,
                        'NatGatewayId': nat['NatGatewayId'],
                        'State': nat['State'],
                        'VpcId': nat['VpcId'],
                        'SubnetId': nat['SubnetId'],
                        'PublicIp': nat['NatGatewayAddresses'][0]['PublicIp'] if nat.get('NatGatewayAddresses') else 'N/A',
                        'CreatedTime': nat['CreateTime'].isoformat(),
                        'Name': tags.get('Name', 'N/A')
                    })
                    
            except Exception as e:
                self.log_error('nat-gateway', region, e)
        
        return nat_gateways
    
    def list_elastic_ips(self):
        """Listar Elastic IPs"""
        eips = []
        regions = self.get_all_regions()
        
        for region in regions:
            try:
                ec2 = boto3.client('ec2', region_name=region)
                response = ec2.describe_addresses()
                
                for eip in response['Addresses']:
                    tags = {tag['Key']: tag['Value'] for tag in eip.get('Tags', [])}
                    
                    eips.append({
                        'Region': region,
                        'PublicIp': eip['PublicIp'],
                        'AllocationId': eip.get('AllocationId', 'N/A'),
                        'AssociationId': eip.get('AssociationId', 'N/A'),
                        'InstanceId': eip.get('InstanceId', 'N/A'),
                        'Domain': eip['Domain'],
                        'Name': tags.get('Name', 'N/A')
                    })
                    
            except Exception as e:
                self.log_error('elastic-ip', region, e)
        
        return eips
    
    def list_cloudfront_distributions(self):
        """Listar distribuições CloudFront"""
        distributions = []
        try:
            cf = boto3.client('cloudfront')
            response = cf.list_distributions()
            
            if 'DistributionList' in response and response['DistributionList']['Quantity'] > 0:
                for dist in response['DistributionList']['Items']:
                    distributions.append({
                        'Id': dist['Id'],
                        'DomainName': dist['DomainName'],
                        'Status': dist['Status'],
                        'Enabled': dist['Enabled'],
                        'PriceClass': dist['PriceClass'],
                        'Origins': len(dist['Origins']['Items']),
                        'LastModifiedTime': dist['LastModifiedTime'].isoformat()
                    })
                    
        except Exception as e:
            self.log_error('cloudfront', 'global', e)
        
        return distributions
    
    def list_additional_cost_resources(self):
        """Listar recursos adicionais que geram custos"""
        additional_resources = {}
        
        # EFS (Elastic File System)
        print("Coletando sistemas EFS...")
        additional_resources['EFS_FileSystems'] = self.list_efs_filesystems()
        
        # ElastiCache
        print("Coletando clusters ElastiCache...")
        additional_resources['ElastiCache_Clusters'] = self.list_elasticache_clusters()
        
        # Redshift
        print("Coletando clusters Redshift...")
        additional_resources['Redshift_Clusters'] = self.list_redshift_clusters()
        
        # OpenSearch/Elasticsearch
        print("Coletando domínios OpenSearch...")
        additional_resources['OpenSearch_Domains'] = self.list_opensearch_domains()
        
        # API Gateway
        print("Coletando APIs Gateway...")
        additional_resources['API_Gateways'] = self.list_api_gateways()
        
        # ECS/Fargate
        print("Coletando clusters ECS...")
        additional_resources['ECS_Clusters'] = self.list_ecs_clusters()
        
        # EKS
        print("Coletando clusters EKS...")
        additional_resources['EKS_Clusters'] = self.list_eks_clusters()
        
        return additional_resources

    def list_efs_filesystems(self):
        """Listar sistemas de arquivos EFS"""
        filesystems = []
        regions = self.get_all_regions()
        
        for region in regions:
            try:
                efs = boto3.client('efs', region_name=region)
                response = efs.describe_file_systems()
                
                for fs in response['FileSystems']:
                    filesystems.append({
                        'Region': region,
                        'FileSystemId': fs['FileSystemId'],
                        'State': fs['LifeCycleState'],
                        'SizeBytes': fs.get('SizeInBytes', {}).get('Value', 0),
                        'ThroughputMode': fs.get('ThroughputMode', 'N/A'),
                        'PerformanceMode': fs.get('PerformanceMode', 'N/A'),
                        'CreationTime': fs['CreationTime'].isoformat(),
                        'NumberOfMountTargets': fs.get('NumberOfMountTargets', 0)
                    })
            except Exception as e:
                self.log_error('efs', region, e)
        
        return filesystems

    def list_elasticache_clusters(self):
        """Listar clusters ElastiCache"""
        clusters = []
        regions = self.get_all_regions()
        
        for region in regions:
            try:
                ec = boto3.client('elasticache', region_name=region)
                
                # Redis clusters
                redis_clusters = ec.describe_cache_clusters()
                for cluster in redis_clusters['CacheClusters']:
                    clusters.append({
                        'Region': region,
                        'ClusterId': cluster['CacheClusterId'],
                        'Engine': cluster['Engine'],
                        'EngineVersion': cluster['EngineVersion'],
                        'NodeType': cluster['CacheNodeType'],
                        'Status': cluster['CacheClusterStatus'],
                        'NumNodes': cluster['NumCacheNodes'],
                        'Type': 'Cache Cluster'
                    })
                
                # Replication groups
                repl_groups = ec.describe_replication_groups()
                for group in repl_groups['ReplicationGroups']:
                    clusters.append({
                        'Region': region,
                        'ReplicationGroupId': group['ReplicationGroupId'],
                        'Status': group['Status'],
                        'NodeType': group.get('CacheNodeType', 'N/A'),
                        'NumNodeGroups': len(group.get('NodeGroups', [])),
                        'Type': 'Replication Group'
                    })
                    
            except Exception as e:
                self.log_error('elasticache', region, e)
        
        return clusters

    def list_redshift_clusters(self):
        """Listar clusters Redshift"""
        clusters = []
        regions = self.get_all_regions()
        
        for region in regions:
            try:
                rs = boto3.client('redshift', region_name=region)
                response = rs.describe_clusters()
                
                for cluster in response['Clusters']:
                    clusters.append({
                        'Region': region,
                        'ClusterIdentifier': cluster['ClusterIdentifier'],
                        'NodeType': cluster['NodeType'],
                        'ClusterStatus': cluster['ClusterStatus'],
                        'NumberOfNodes': cluster['NumberOfNodes'],
                        'DBName': cluster.get('DBName', 'N/A'),
                        'PubliclyAccessible': cluster.get('PubliclyAccessible', False),
                        'ClusterCreateTime': cluster['ClusterCreateTime'].isoformat()
                    })
            except Exception as e:
                self.log_error('redshift', region, e)
        
        return clusters

    def list_opensearch_domains(self):
        """Listar domínios OpenSearch"""
        domains = []
        regions = self.get_all_regions()
        
        for region in regions:
            try:
                opensearch = boto3.client('opensearch', region_name=region)
                response = opensearch.list_domain_names()
                
                for domain in response['DomainNames']:
                    domain_name = domain['DomainName']
                    
                    # Obter detalhes do domínio
                    details = opensearch.describe_domain(DomainName=domain_name)
                    domain_detail = details['DomainStatus']
                    
                    domains.append({
                        'Region': region,
                        'DomainName': domain_name,
                        'ElasticsearchVersion': domain_detail.get('ElasticsearchVersion', 'N/A'),
                        'Created': domain_detail.get('Created', False),
                        'Processing': domain_detail.get('Processing', False),
                        'InstanceType': domain_detail.get('ClusterConfig', {}).get('InstanceType', 'N/A'),
                        'InstanceCount': domain_detail.get('ClusterConfig', {}).get('InstanceCount', 0)
                    })
            except Exception as e:
                self.log_error('opensearch', region, e)
        
        return domains

    def list_api_gateways(self):
        """Listar APIs Gateway"""
        apis = []
        regions = self.get_all_regions()
        
        for region in regions:
            try:
                apigw = boto3.client('apigateway', region_name=region)
                response = apigw.get_rest_apis()
                
                for api in response['items']:
                    apis.append({
                        'Region': region,
                        'Id': api['id'],
                        'Name': api['name'],
                        'Description': api.get('description', 'N/A'),
                        'CreatedDate': api['createdDate'].isoformat(),
                        'EndpointType': api.get('endpointConfiguration', {}).get('types', ['N/A'])
                    })
            except Exception as e:
                self.log_error('apigateway', region, e)
        
        return apis

    def list_ecs_clusters(self):
        """Listar clusters ECS"""
        clusters = []
        regions = self.get_all_regions()
        
        for region in regions:
            try:
                ecs = boto3.client('ecs', region_name=region)
                cluster_arns = ecs.list_clusters()['clusterArns']
                
                if cluster_arns:
                    cluster_details = ecs.describe_clusters(clusters=cluster_arns)
                    
                    for cluster in cluster_details['clusters']:
                        clusters.append({
                            'Region': region,
                            'ClusterName': cluster['clusterName'],
                            'Status': cluster['status'],
                            'RunningTasksCount': cluster['runningTasksCount'],
                            'PendingTasksCount': cluster['pendingTasksCount'],
                            'ActiveServicesCount': cluster['activeServicesCount']
                        })
            except Exception as e:
                self.log_error('ecs', region, e)
        
        return clusters

    def list_eks_clusters(self):
        """Listar clusters EKS"""
        clusters = []
        regions = self.get_all_regions()
        
        for region in regions:
            try:
                eks = boto3.client('eks', region_name=region)
                response = eks.list_clusters()
                
                for cluster_name in response['clusters']:
                    cluster_detail = eks.describe_cluster(name=cluster_name)
                    cluster = cluster_detail['cluster']
                    
                    clusters.append({
                        'Region': region,
                        'Name': cluster['name'],
                        'Status': cluster['status'],
                        'Version': cluster['version'],
                        'PlatformVersion': cluster['platformVersion'],
                        'CreatedAt': cluster['createdAt'].isoformat(),
                        'Endpoint': cluster.get('endpoint', 'N/A')
                    })
            except Exception as e:
                self.log_error('eks', region, e)
        
        return clusters

    def run_inventory(self):
        """Executar inventário completo"""
        print("Iniciando inventário COMPLETO de recursos AWS...")
        
        print("Coletando instâncias EC2...")
        self.inventory['EC2_Instances'] = self.list_ec2_instances()
        
        print("Coletando volumes EBS...")
        self.inventory['EBS_Volumes'] = self.list_ebs_volumes()
        
        print("Coletando buckets S3...")
        self.inventory['S3_Buckets'] = self.list_s3_buckets()
        
        print("Coletando instâncias RDS...")
        self.inventory['RDS_Instances'] = self.list_rds_instances()
        
        print("Coletando Load Balancers...")
        self.inventory['Load_Balancers'] = self.list_load_balancers()
        
        print("Coletando funções Lambda...")
        self.inventory['Lambda_Functions'] = self.list_lambda_functions()
        
        print("Coletando NAT Gateways...")
        self.inventory['NAT_Gateways'] = self.list_nat_gateways()
        
        print("Coletando Elastic IPs...")
        self.inventory['Elastic_IPs'] = self.list_elastic_ips()
        
        print("Coletando distribuições CloudFront...")
        self.inventory['CloudFront_Distributions'] = self.list_cloudfront_distributions()
        
        # Recursos adicionais
        additional = self.list_additional_cost_resources()
        self.inventory.update(additional)
        
        # Adicionar timestamp e resumo
        self.inventory['_metadata'] = {
            'generated_at': datetime.utcnow().isoformat(),
            'total_errors': len(self.errors),
            'summary': {
                'ec2_instances': len(self.inventory.get('EC2_Instances', [])),
                'ebs_volumes': len(self.inventory.get('EBS_Volumes', [])),
                's3_buckets': len(self.inventory.get('S3_Buckets', [])),
                'rds_instances': len(self.inventory.get('RDS_Instances', [])),
                'load_balancers': len(self.inventory.get('Load_Balancers', [])),
                'lambda_functions': len(self.inventory.get('Lambda_Functions', [])),
                'nat_gateways': len(self.inventory.get('NAT_Gateways', [])),
                'elastic_ips': len(self.inventory.get('Elastic_IPs', [])),
                'cloudfront_distributions': len(self.inventory.get('CloudFront_Distributions', [])),
                'efs_filesystems': len(self.inventory.get('EFS_FileSystems', [])),
                'elasticache_clusters': len(self.inventory.get('ElastiCache_Clusters', [])),
                'redshift_clusters': len(self.inventory.get('Redshift_Clusters', [])),
                'opensearch_domains': len(self.inventory.get('OpenSearch_Domains', [])),
                'api_gateways': len(self.inventory.get('API_Gateways', [])),
                'ecs_clusters': len(self.inventory.get('ECS_Clusters', [])),
                'eks_clusters': len(self.inventory.get('EKS_Clusters', []))
            }
        }
        
        if self.errors:
            self.inventory['_errors'] = self.errors
        
        return self.inventory

def main():
    try:
        # Verificar credenciais AWS
        sts = boto3.client('sts')
        identity = sts.get_caller_identity()
        print(f"✅ Credenciais AWS válidas - Account: {identity['Account']}, User: {identity.get('Arn', 'N/A')}")
        
    except (NoCredentialsError, ClientError) as e:
        print(f"❌ Erro nas credenciais AWS: {e}")
        print("💡 Certifique-se de estar executando no CloudShell ou com credenciais configuradas")
        sys.exit(1)
    
    # Executar inventário
    inventory = AWSCostInventory()
    results = inventory.run_inventory()
    
    # Salvar resultado
    output_file = 'aws_cost_inventory.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"\n✅ Inventário completo salvo em '{output_file}'")
    print(f"📊 Resumo:")
    for service, count in results['_metadata']['summary'].items():
        print(f"   • {service.replace('_', ' ').title()}: {count}")
    
    if results.get('_errors'):
        print(f"⚠️ Total de erros encontrados: {len(results['_errors'])}")
    
    print(f"\n📄 Para visualizar o resultado completo:")
    print(f"   cat {output_file} | jq .")

if __name__ == "__main__":
    main()
