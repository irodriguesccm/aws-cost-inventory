import boto3
import json
import os
from datetime import datetime, timedelta
from botocore.exceptions import ClientError, NoCredentialsError
from botocore.config import Config
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError
import signal

class AWSCostInventory:
    def __init__(self):
        self.inventory = {}
        self.errors = []
        self.failed_regions = set()
        self.max_s3_objects = 10000  # Limite para evitar travamentos
        self.timeout_seconds = 30
        
        # Configuração de timeout para clientes boto3
        self.boto_config = Config(
            region_name='us-east-1',
            retries={
                'max_attempts': 3,
                'mode': 'adaptive'
            },
            read_timeout=self.timeout_seconds,
            connect_timeout=self.timeout_seconds
        )
        
    def create_client(self, service_name, region_name=None):
        """Criar cliente AWS com configuração de timeout"""
        config = Config(
            region_name=region_name or 'us-east-1',
            retries={
                'max_attempts': 3,
                'mode': 'adaptive'
            },
            read_timeout=self.timeout_seconds,
            connect_timeout=self.timeout_seconds
        )
        return boto3.client(service_name, region_name=region_name, config=config)
    
    def with_timeout(self, func, timeout=None, *args, **kwargs):
        """Executar função com timeout usando threading"""
        if timeout is None:
            timeout = self.timeout_seconds
            
        result = [None]
        exception = [None]
        
        def target():
            try:
                result[0] = func(*args, **kwargs)
            except Exception as e:
                exception[0] = e
        
        thread = threading.Thread(target=target)
        thread.daemon = True
        thread.start()
        thread.join(timeout)
        
        if thread.is_alive():
            # Thread ainda rodando - timeout
            raise TimeoutError(f"Operação expirou após {timeout} segundos")
        
        if exception[0]:
            raise exception[0]
        
        return result[0]
    
    def log_error(self, service, region, error):
        """Log de erros para análise posterior"""
        self.errors.append({
            "service": service,
            "region": region,
            "error": str(error),
            "timestamp": datetime.utcnow().isoformat()
        })
        print(f" Erro {service} ({region}): {str(error)[:100]}...")
    
    def get_all_regions(self):
        """Obter todas as regiões AWS disponíveis com fallback"""
        try:
            ec2 = self.create_client('ec2', 'us-east-1')
            regions = [r['RegionName'] for r in ec2.describe_regions()['Regions']]
            print(f" Encontradas {len(regions)} regiões AWS")
            return regions
        except Exception as e:
            self.log_error('ec2', 'global', e)
            # Fallback com principais regiões
            fallback_regions = [
                'us-east-1', 'us-west-2', 'eu-west-1', 'ap-northeast-1',
                'us-west-1', 'eu-central-1', 'ap-southeast-1', 'sa-east-1'
            ]
            print(f" Usando regiões fallback: {len(fallback_regions)}")
            return fallback_regions
    
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
        """Listar buckets S3 com timeouts e otimizações"""
        buckets = []
        try:
            s3 = self.create_client('s3')
            response = s3.list_buckets()
            
            total_buckets = len(response['Buckets'])
            print(f" Encontrados {total_buckets} buckets S3")
            
            for i, bucket in enumerate(response['Buckets'], 1):
                bucket_name = bucket['Name']
                print(f" [{i}/{total_buckets}] Analisando bucket: {bucket_name}")
                
                # Obter região do bucket com timeout
                try:
                    bucket_region = s3.get_bucket_location(Bucket=bucket_name)['LocationConstraint']
                    if bucket_region is None:
                        bucket_region = 'us-east-1'
                except Exception as e:
                    print(f"    Erro ao obter região: {str(e)[:50]}...")
                    bucket_region = 'unknown'
                
                # Obter tamanho com timeout e limitações
                size_info = self.get_bucket_size(bucket_name, bucket_region)
                
                buckets.append({
                    'BucketName': bucket_name,
                    'Region': bucket_region,
                    'CreationDate': bucket['CreationDate'].isoformat(),
                    'Size': size_info['formatted_size'],
                    'ObjectCount': size_info['object_count'],
                    'SizeBytes': size_info['size_bytes'],
                    'AnalysisMethod': size_info['method_used']
                })
                
        except Exception as e:
            self.log_error('s3', 'global', e)
        
        return buckets

    def get_bucket_size(self, bucket_name, bucket_region):
        """Obter tamanho do bucket com timeout e limite de objetos"""
        size_info = {
            'size_bytes': 0,
            'object_count': 0,
            'formatted_size': 'N/A',
            'method_used': 'none'
        }
        
        try:
            print(f"    Analisando bucket {bucket_name} (timeout: {self.timeout_seconds}s)...")
            
            # Estratégia 1: Tentar CloudWatch primeiro (mais rápido)
            try:
                cw_region = 'us-east-1' if bucket_region in [None, 'us-east-1'] else bucket_region
                cw = self.create_client('cloudwatch', cw_region)
                
                # Tentar obter métricas dos últimos 2 dias
                metric = cw.get_metric_statistics(
                    Namespace='AWS/S3',
                    MetricName='BucketSizeBytes',
                    Dimensions=[
                        {'Name': 'BucketName', 'Value': bucket_name},
                        {'Name': 'StorageType', 'Value': 'StandardStorage'}
                    ],
                    StartTime=datetime.utcnow() - timedelta(days=2),
                    EndTime=datetime.utcnow(),
                    Period=86400,
                    Statistics=['Average']
                )
                
                if metric['Datapoints']:
                    size_bytes = metric['Datapoints'][-1]['Average']
                    size_info['size_bytes'] = size_bytes
                    size_info['method_used'] = 'cloudwatch'
                    
                    # Tentar obter contagem de objetos
                    count_metric = cw.get_metric_statistics(
                        Namespace='AWS/S3',
                        MetricName='NumberOfObjects',
                        Dimensions=[
                            {'Name': 'BucketName', 'Value': bucket_name},
                            {'Name': 'StorageType', 'Value': 'AllStorageTypes'}
                        ],
                        StartTime=datetime.utcnow() - timedelta(days=2),
                        EndTime=datetime.utcnow(),
                        Period=86400,
                        Statistics=['Average']
                    )
                    
                    if count_metric['Datapoints']:
                        size_info['object_count'] = int(count_metric['Datapoints'][-1]['Average'])
                    
                    size_info['formatted_size'] = self.format_bytes(size_bytes)
                    print(f"    CloudWatch {bucket_name}: {size_info['formatted_size']}")
                    return size_info
                    
            except Exception as cw_error:
                print(f"    CloudWatch falhou para {bucket_name}: {str(cw_error)[:50]}...")
                pass
            
            # Estratégia 2: Listagem direta com limite e timeout
            def list_objects_limited():
                s3 = self.create_client('s3')
                paginator = s3.get_paginator('list_objects_v2')
                
                total_size = 0
                total_count = 0
                
                print(f"    Listando objetos (máx: {self.max_s3_objects})...")
                
                for page in paginator.paginate(Bucket=bucket_name):
                    if 'Contents' in page:
                        for obj in page['Contents']:
                            total_size += obj['Size']
                            total_count += 1
                            
                            # Parar se atingir o limite
                            if total_count >= self.max_s3_objects:
                                print(f"    Limite de {self.max_s3_objects} objetos atingido")
                                return total_size, total_count, True
                
                return total_size, total_count, False
            
            # Executar com timeout de 60 segundos para S3
            size_bytes, count, limited = self.with_timeout(list_objects_limited, 60)
            
            size_info['size_bytes'] = size_bytes
            size_info['object_count'] = count
            size_info['method_used'] = 'direct_limited' if limited else 'direct_full'
            
            if limited:
                size_info['formatted_size'] = f"{self.format_bytes(size_bytes)} (>{self.max_s3_objects} objs)"
            else:
                size_info['formatted_size'] = f"{self.format_bytes(size_bytes)} ({count} objs)"
            
            print(f"    Listagem {bucket_name}: {size_info['formatted_size']}")
            return size_info
            
        except TimeoutError:
            print(f"    Timeout no bucket {bucket_name}")
            size_info['formatted_size'] = "Timeout na consulta"
            size_info['method_used'] = 'timeout'
            self.log_error('s3-timeout', bucket_name, "Timeout na consulta do bucket")
            
        except Exception as e:
            print(f"    Erro no bucket {bucket_name}: {str(e)[:50]}...")
            size_info['formatted_size'] = "Erro ao obter tamanho"
            size_info['method_used'] = 'error'
            self.log_error('s3-bucket-size', bucket_name, e)
        
        return size_info
    
    def format_bytes(self, bytes_value):
        """Formatar bytes para formato legível"""
        if bytes_value == 0:
            return "0 B"
        elif bytes_value < 1024:
            return f"{bytes_value:.0f} B"
        elif bytes_value < 1024**2:
            return f"{bytes_value/1024:.2f} KB"
        elif bytes_value < 1024**3:
            return f"{bytes_value/(1024**2):.2f} MB"
        elif bytes_value < 1024**4:
            return f"{bytes_value/(1024**3):.2f} GB"
        else:
            return f"{bytes_value/(1024**4):.2f} TB"
    
    def list_rds_instances(self):
        """Listar instâncias RDS com detalhamento completo de clusters Aurora"""
        rds_instances = []
        regions = self.get_all_regions()
        
        for region in regions:
            try:
                rds = boto3.client('rds', region_name=region)
                
                # RDS Instances standalone
                instances = rds.describe_db_instances()
                for instance in instances['DBInstances']:
                    # Verifica se não é parte de um cluster Aurora
                    if not instance.get('DBClusterIdentifier'):
                        rds_instances.append({
                            'Region': region,
                            'Type': 'RDS Instance',
                            'DBInstanceIdentifier': instance['DBInstanceIdentifier'],
                            'DBInstanceClass': instance['DBInstanceClass'],
                            'Engine': instance['Engine'],
                            'EngineVersion': instance['EngineVersion'],
                            'DBInstanceStatus': instance['DBInstanceStatus'],
                            'AllocatedStorage': instance.get('AllocatedStorage', 0),
                            'StorageType': instance.get('StorageType', 'N/A'),
                            'StorageEncrypted': instance.get('StorageEncrypted', False),
                            'MultiAZ': instance['MultiAZ'],
                            'PubliclyAccessible': instance['PubliclyAccessible'],
                            'AvailabilityZone': instance.get('AvailabilityZone', 'N/A'),
                            'VpcId': instance.get('DbSubnetGroup', {}).get('VpcId', 'N/A'),
                            'SubnetGroupName': instance.get('DbSubnetGroup', {}).get('DBSubnetGroupName', 'N/A'),
                            'BackupRetentionPeriod': instance.get('BackupRetentionPeriod', 0),
                            'MonitoringInterval': instance.get('MonitoringInterval', 0),
                            'PerformanceInsightsEnabled': instance.get('PerformanceInsightsEnabled', False),
                            'DeletionProtection': instance.get('DeletionProtection', False)
                        })
                    
                # RDS Clusters (Aurora) com detalhamento completo
                try:
                    clusters = rds.describe_db_clusters()
                    for cluster in clusters['DBClusters']:
                        # Obter detalhes das instâncias do cluster
                        cluster_instances = []
                        cluster_members = cluster.get('DBClusterMembers', [])
                        
                        # Buscar informações detalhadas de cada instância do cluster
                        for member in cluster_members:
                            instance_id = member['DBInstanceIdentifier']
                            try:
                                instance_detail = rds.describe_db_instances(DBInstanceIdentifier=instance_id)
                                instance_info = instance_detail['DBInstances'][0]
                                
                                cluster_instances.append({
                                    'DBInstanceIdentifier': instance_id,
                                    'DBInstanceClass': instance_info['DBInstanceClass'],
                                    'DBInstanceStatus': instance_info['DBInstanceStatus'],
                                    'AvailabilityZone': instance_info.get('AvailabilityZone', 'N/A'),
                                    'IsClusterWriter': member.get('IsClusterWriter', False),
                                    'PromotionTier': member.get('PromotionTier', 'N/A'),
                                    'MonitoringInterval': instance_info.get('MonitoringInterval', 0),
                                    'PerformanceInsightsEnabled': instance_info.get('PerformanceInsightsEnabled', False),
                                    'PubliclyAccessible': instance_info.get('PubliclyAccessible', False)
                                })
                            except Exception as e:
                                self.log_error(f'rds-cluster-instance-{instance_id}', region, e)
                                cluster_instances.append({
                                    'DBInstanceIdentifier': instance_id,
                                    'IsClusterWriter': member.get('IsClusterWriter', False),
                                    'PromotionTier': member.get('PromotionTier', 'N/A'),
                                    'Status': 'Error retrieving details'
                                })
                        
                        # Calcular configurações de capacidade para Serverless
                        serverless_config = {}
                        if cluster.get('EngineMode') == 'serverless':
                            scaling_config = cluster.get('ScalingConfigurationInfo', {})
                            serverless_config = {
                                'MinCapacity': scaling_config.get('MinCapacity', 'N/A'),
                                'MaxCapacity': scaling_config.get('MaxCapacity', 'N/A'),
                                'AutoPause': scaling_config.get('AutoPause', False),
                                'SecondsUntilAutoPause': scaling_config.get('SecondsUntilAutoPause', 'N/A')
                            }
                        
                        # Informações de storage do cluster
                        storage_info = {
                            'StorageEncrypted': cluster.get('StorageEncrypted', False),
                            'KmsKeyId': cluster.get('KmsKeyId', 'N/A') if cluster.get('StorageEncrypted') else 'N/A',
                            'AllocatedStorage': cluster.get('AllocatedStorage', 0),
                            'StorageType': cluster.get('StorageType', 'Aurora')
                        }
                        
                        rds_instances.append({
                            'Region': region,
                            'Type': 'Aurora Cluster',
                            'DBClusterIdentifier': cluster['DBClusterIdentifier'],
                            'Engine': cluster['Engine'],
                            'EngineVersion': cluster['EngineVersion'],
                            'EngineMode': cluster.get('EngineMode', 'provisioned'),
                            'Status': cluster['Status'],
                            'DatabaseName': cluster.get('DatabaseName', 'N/A'),
                            'MasterUsername': cluster['MasterUsername'],
                            'Port': cluster.get('Port', 'N/A'),
                            'Endpoint': cluster.get('Endpoint', 'N/A'),
                            'ReaderEndpoint': cluster.get('ReaderEndpoint', 'N/A'),
                            'MultiAZ': cluster.get('MultiAZ', False),
                            'VpcId': cluster.get('DbSubnetGroup', {}).get('VpcId', 'N/A'),
                            'SubnetGroupName': cluster.get('DbSubnetGroup', {}).get('DBSubnetGroupName', 'N/A'),
                            'BackupRetentionPeriod': cluster.get('BackupRetentionPeriod', 0),
                            'PreferredBackupWindow': cluster.get('PreferredBackupWindow', 'N/A'),
                            'PreferredMaintenanceWindow': cluster.get('PreferredMaintenanceWindow', 'N/A'),
                            'DeletionProtection': cluster.get('DeletionProtection', False),
                            'HttpEndpointEnabled': cluster.get('HttpEndpointEnabled', False),
                            'ClusterMembers': len(cluster_members),
                            'ClusterInstances': cluster_instances,
                            'ServerlessConfig': serverless_config,
                            'StorageInfo': storage_info,
                            'AvailabilityZones': list(set([inst.get('AvailabilityZone', 'N/A') for inst in cluster_instances if inst.get('AvailabilityZone') != 'N/A'])),
                            'TotalInstanceClasses': list(set([inst.get('DBInstanceClass', 'N/A') for inst in cluster_instances if inst.get('DBInstanceClass') != 'N/A'])),
                            'WriterInstances': len([inst for inst in cluster_instances if inst.get('IsClusterWriter', False)]),
                            'ReaderInstances': len([inst for inst in cluster_instances if not inst.get('IsClusterWriter', False)])
                        })
                except Exception as e:
                    self.log_error('rds-clusters', region, e)
                    
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
        """Listar funções Lambda com timeout"""
        functions = []
        regions = self.get_all_regions()
        
        def list_lambda_region(region):
            region_functions = []
            try:
                lambda_client = self.create_client('lambda', region)
                response = lambda_client.list_functions()
                
                for func in response['Functions']:
                    region_functions.append({
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
            return region_functions
        
        return self.process_region_parallel(list_lambda_region, regions, 'Lambda', max_workers=3)
    
    def list_nat_gateways(self):
        """Listar NAT Gateways"""
        nat_gateways = []
        regions = self.get_all_regions()
        
        for region in regions:
            try:
                ec2 = self.create_client('ec2', region)
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
    
    def process_region_parallel(self, service_func, regions, service_name, max_workers=3):
        """Processar regiões em paralelo com limite de workers"""
        all_results = []
        failed_regions = []
        
        print(f" Processando {service_name} em {len(regions)} regiões (workers: {max_workers})...")
        
        def process_single_region(region):
            try:
                if region in self.failed_regions:
                    print(f"    Pulando região {region} (falha anterior)")
                    return []
                
                print(f"    Processando {service_name} em {region}...")
                result = service_func(region)
                print(f"    {region}: {len(result) if isinstance(result, list) else 'OK'}")
                return result
            except Exception as e:
                error_msg = str(e)[:100]
                print(f"    {region}: {error_msg}")
                self.log_error(service_name, region, e)
                failed_regions.append(region)
                return []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_region = {executor.submit(process_single_region, region): region 
                              for region in regions}
            
            for future in future_to_region:
                try:
                    result = future.result(timeout=self.timeout_seconds * 2)  # Timeout maior para paralelo
                    if result:
                        all_results.extend(result if isinstance(result, list) else [result])
                except TimeoutError:
                    region = future_to_region[future]
                    print(f"    Timeout em {region}")
                    failed_regions.append(region)
                except Exception as e:
                    region = future_to_region[future]
                    print(f"    Erro em {region}: {str(e)[:50]}")
                    failed_regions.append(region)
        
        # Marcar regiões que falharam consistentemente
        for region in failed_regions:
            self.failed_regions.add(region)
        
        print(f" {service_name}: {len(all_results)} recursos encontrados")
        if failed_regions:
            print(f" Falhas em {len(failed_regions)} regiões: {failed_regions[:3]}...")
        
        return all_results
    
    def list_ec2_instances_region(self, region):
        """Listar instâncias EC2 em uma região específica"""
        instances = []
        try:
            ec2 = self.create_client('ec2', region)
            response = ec2.describe_instances()
            
            for reservation in response['Reservations']:
                for instance in reservation['Instances']:
                    tags = {tag['Key']: tag['Value'] for tag in instance.get('Tags', [])}
                    
                    # Obter volumes de forma mais eficiente
                    volumes = []
                    if 'BlockDeviceMappings' in instance:
                        for mapping in instance['BlockDeviceMappings'][:5]:  # Limitar a 5 volumes
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
                                except Exception:
                                    pass  # Ignorar erros de volume individual
                    
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
    
    def list_ec2_instances(self):
        """Listar instâncias EC2 usando processamento paralelo"""
        regions = self.get_all_regions()
        return self.process_region_parallel(
            self.list_ec2_instances_region, 
            regions, 
            'EC2', 
            max_workers=4
        )
    
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
        """Executar inventário completo com timeouts e progresso"""
        start_time = time.time()
        print(" Iniciando inventário COMPLETO de recursos AWS...")
        print(f"  Timeout por operação: {self.timeout_seconds}s")
        print(f" Limite S3 por bucket: {self.max_s3_objects} objetos")
        print("=" * 60)
        
        services = [
            ('EC2_Instances', self.list_ec2_instances, "  Instâncias EC2"),
            ('EBS_Volumes', self.list_ebs_volumes, " Volumes EBS"),
            ('S3_Buckets', self.list_s3_buckets, " Buckets S3"),
            ('RDS_Instances', self.list_rds_instances, "  Instâncias RDS"),
            ('Load_Balancers', self.list_load_balancers, "  Load Balancers"),
            ('Lambda_Functions', self.list_lambda_functions, " Funções Lambda"),
            ('NAT_Gateways', self.list_nat_gateways, " NAT Gateways"),
            ('Elastic_IPs', self.list_elastic_ips, " Elastic IPs"),
            ('CloudFront_Distributions', self.list_cloudfront_distributions, "  CloudFront")
        ]
        
        total_services = len(services)
        
        for i, (key, func, description) in enumerate(services, 1):
            service_start = time.time()
            print(f"\n[{i}/{total_services}] {description}...")
            
            try:
                # Executar com timeout global
                result = self.with_timeout(func, self.timeout_seconds * 3)
                self.inventory[key] = result
                
                service_time = time.time() - service_start
                count = len(result) if isinstance(result, list) else 1
                print(f" {description}: {count} recursos em {service_time:.1f}s")
                
            except TimeoutError:
                print(f" TIMEOUT: {description} excedeu {self.timeout_seconds * 3}s")
                self.inventory[key] = []
                self.log_error(key.lower(), 'global', 'Timeout global do serviço')
                
            except Exception as e:
                print(f" ERRO: {description} - {str(e)[:100]}...")
                self.inventory[key] = []
                self.log_error(key.lower(), 'global', e)
        
        # Recursos adicionais com timeout individual
        print(f"\n[{total_services + 1}]  Recursos adicionais...")
        try:
            additional = self.with_timeout(self.list_additional_cost_resources, self.timeout_seconds * 5)
            self.inventory.update(additional)
            print(" Recursos adicionais coletados")
        except TimeoutError:
            print(f" TIMEOUT: Recursos adicionais excederam {self.timeout_seconds * 5}s")
        except Exception as e:
            print(f" ERRO: Recursos adicionais - {str(e)[:100]}...")
        
        # Finalizar
        total_time = time.time() - start_time
        self._add_metadata(total_time)
        
        print(f"\n Inventário concluído em {total_time:.1f}s")
        print(f"  Total de erros: {len(self.errors)}")
        print(f" Regiões com falhas: {len(self.failed_regions)}")
        
        return self.inventory
    
    def _add_metadata(self, total_time):
        """Adicionar metadados ao inventário"""
        self.inventory['_metadata'] = {
            'generated_at': datetime.utcnow().isoformat(),
            'execution_time_seconds': round(total_time, 2),
            'timeout_config': self.timeout_seconds,
            'max_s3_objects': self.max_s3_objects,
            'total_errors': len(self.errors),
            'failed_regions': list(self.failed_regions),
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

def main():
    try:
        print(" Verificando credenciais AWS...")
        # Verificar credenciais AWS com timeout
        sts = boto3.client('sts', config=Config(
            retries={'max_attempts': 3, 'mode': 'adaptive'},
            read_timeout=15,
            connect_timeout=15
        ))
        identity = sts.get_caller_identity()
        print(f" Credenciais AWS válidas")
        print(f"   Account: {identity['Account']}")
        print(f"   User: {identity.get('Arn', 'N/A')}")
        
    except (NoCredentialsError, ClientError) as e:
        print(f" Erro nas credenciais AWS: {e}")
        print(" Certifique-se de estar executando no CloudShell ou com credenciais configuradas")
        sys.exit(1)
    except Exception as e:
        print(f" Erro inesperado na verificação de credenciais: {e}")
        print(" Continuando mesmo assim...")
    
    # Executar inventário
    print("\n" + "=" * 60)
    inventory = AWSCostInventory()
    
    # Ajustar timeouts baseado no ambiente
    # CloudShell tem limitações, então usar timeouts mais conservadores
    if 'AWS_EXECUTION_ENV' in os.environ:
        print("  Detectado ambiente CloudShell - usando timeouts conservadores")
        inventory.timeout_seconds = 20
        inventory.max_s3_objects = 5000
    
    results = inventory.run_inventory()
    
    # Salvar resultado
    output_file = f'aws_cost_inventory.json'
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"\n Inventário salvo em '{output_file}'")
        
        # Mostrar resumo
        print(f"\n RESUMO DO INVENTÁRIO:")
        print("=" * 40)
        metadata = results.get('_metadata', {})
        summary = metadata.get('summary', {})
        
        for service, count in summary.items():
            if count > 0:
                service_name = service.replace('_', ' ').title()
                print(f"   • {service_name}: {count}")
        
        print(f"\n  Tempo total: {metadata.get('execution_time_seconds', 'N/A')}s")
        
        if results.get('_errors'):
            error_count = len(results['_errors'])
            print(f"  Erros encontrados: {error_count}")
            
            # Mostrar tipos de erro mais comuns
            error_services = {}
            for error in results['_errors'][:10]:  # Primeiros 10 erros
                service = error['service']
                error_services[service] = error_services.get(service, 0) + 1
            
            if error_services:
                print("   Serviços com mais erros:")
                for service, count in sorted(error_services.items(), key=lambda x: x[1], reverse=True):
                    print(f"     - {service}: {count} erros")
        
        failed_regions = metadata.get('failed_regions', [])
        if failed_regions:
            print(f" Regiões com falhas: {len(failed_regions)}")
            if len(failed_regions) <= 5:
                print(f"   {', '.join(failed_regions)}")
            else:
                print(f"   {', '.join(failed_regions[:5])} e mais {len(failed_regions) - 5}...")
        
        print(f"\n Para visualizar detalhes:")
        print(f"   cat {output_file} | jq .")
        
    except Exception as e:
        print(f"Erro ao salvar arquivo: {e}")
        print(" Resultado disponível apenas na memória")

if __name__ == "__main__":
    main()
