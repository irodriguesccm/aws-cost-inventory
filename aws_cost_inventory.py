import boto3
import json
from datetime import datetime, timedelta
from botocore.exceptions import ClientError, NoCredentialsError
from botocore.config import Config
import sys
import time
import signal
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
import threading

class AWSCostInventory:
    def __init__(self):
        self.inventory = {}
        self.errors = []
        self.cancelled = False
        self.start_time = time.time()
        
        # Configuração otimizada para APIs AWS
        self.config = Config(
            region_name='us-east-1',
            retries={
                'max_attempts': 2,  # Reduzido para evitar travamentos
                'mode': 'adaptive'
            },
            read_timeout=30,  # Timeout de leitura de 30 segundos
            connect_timeout=10,  # Timeout de conexão de 10 segundos
            max_pool_connections=10
        )
        
        # Regiões prioritárias para evitar timeouts desnecessários
        self.priority_regions = [
            'us-east-1', 'us-west-2', 'eu-west-1', 'eu-central-1',
            'ap-southeast-1', 'ap-northeast-1'
        ]
        
        # Configurar handler para cancelamento
        signal.signal(signal.SIGINT, self.signal_handler)
        
    def signal_handler(self, signum, frame):
        """Handler para cancelamento via Ctrl+C"""
        print("\n⚠️ Recebido sinal de cancelamento. Finalizando operações...")
        self.cancelled = True
        
    def is_cancelled(self):
        """Verificar se a operação foi cancelada"""
        return self.cancelled
        
    def get_elapsed_time(self):
        """Obter tempo decorrido em segundos"""
        return time.time() - self.start_time
        
    def log_error(self, service, region, error):
        """Log de erros para análise posterior"""
        self.errors.append({
            "service": service,
            "region": region,
            "error": str(error),
            "timestamp": datetime.utcnow().isoformat()
        })
    
    def get_all_regions(self):
        """Obter todas as regiões AWS disponíveis com timeout"""
        try:
            ec2 = boto3.client('ec2', region_name='us-east-1', config=self.config)
            response = ec2.describe_regions()
            all_regions = [r['RegionName'] for r in response['Regions']]
            
            # Priorizar regiões mais comuns para evitar timeouts em regiões menos utilizadas
            prioritized = []
            for region in self.priority_regions:
                if region in all_regions:
                    prioritized.append(region)
            
            # Adicionar outras regiões após as prioritárias
            for region in all_regions:
                if region not in prioritized:
                    prioritized.append(region)
                    
            print(f"🌍 Encontradas {len(prioritized)} regiões ({len(self.priority_regions)} prioritárias)")
            return prioritized[:12]  # Limitar a 12 regiões para evitar timeouts
            
        except Exception as e:
            self.log_error('ec2', 'global', e)
            print("⚠️ Erro ao obter regiões, usando fallback básico")
            return self.priority_regions  # fallback com regiões prioritárias
    
    def list_ec2_instances(self):
        """Listar instâncias EC2 com informações de storage - versão otimizada"""
        instances = []
        regions = self.get_all_regions()
        
        print(f"🖥️ Consultando instâncias EC2 em {len(regions)} regiões...")
        
        with ThreadPoolExecutor(max_workers=4) as executor:
            future_to_region = {
                executor.submit(self.get_ec2_instances_by_region, region): region 
                for region in regions
            }
            
            for future in as_completed(future_to_region, timeout=180):
                if self.is_cancelled():
                    break
                    
                region = future_to_region[future]
                try:
                    region_instances = future.result(timeout=30)
                    instances.extend(region_instances)
                    if region_instances:
                        print(f"   ✅ {region}: {len(region_instances)} instâncias")
                except Exception as e:
                    print(f"   ❌ {region}: {str(e)[:50]}")
                    self.log_error('ec2', region, e)
        
        return instances

    def get_ec2_instances_by_region(self, region):
        """Obter instâncias EC2 de uma região específica"""
        instances = []
        try:
            ec2 = boto3.client('ec2', region_name=region, config=self.config)
            response = ec2.describe_instances()
            
            for reservation in response['Reservations']:
                for instance in reservation['Instances']:
                    if self.is_cancelled():
                        break
                        
                    tags = {tag['Key']: tag['Value'] for tag in instance.get('Tags', [])}
                    
                    # Obter volumes de forma otimizada
                    volumes = []
                    if 'BlockDeviceMappings' in instance:
                        volume_ids = [
                            mapping.get('Ebs', {}).get('VolumeId') 
                            for mapping in instance['BlockDeviceMappings'] 
                            if mapping.get('Ebs', {}).get('VolumeId')
                        ]
                        
                        if volume_ids:
                            try:
                                volume_info = ec2.describe_volumes(VolumeIds=volume_ids)
                                for volume in volume_info['Volumes']:
                                    volumes.append({
                                        'VolumeId': volume['VolumeId'],
                                        'Size': volume['Size'],
                                        'VolumeType': volume['VolumeType'],
                                        'Encrypted': volume['Encrypted']
                                    })
                            except Exception:
                                pass  # Ignorar erros de volumes para não travar
                    
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

    def parallel_region_query(self, service_name, query_function, max_workers=3, timeout=120):
        """Executar consultas em paralelo por região com timeout"""
        results = []
        regions = self.get_all_regions()
        
        print(f"🔍 Consultando {service_name} em {len(regions)} regiões...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_region = {
                executor.submit(query_function, region): region 
                for region in regions
            }
            
            completed_count = 0
            for future in as_completed(future_to_region, timeout=timeout):
                if self.is_cancelled():
                    print(f"⚠️ {service_name}: Cancelado pelo usuário")
                    break
                    
                region = future_to_region[future]
                completed_count += 1
                
                try:
                    region_results = future.result(timeout=20)  # 20 segundos por região
                    results.extend(region_results)
                    
                    status = f"({completed_count}/{len(regions)})"
                    if region_results:
                        print(f"   ✅ {region} {status}: {len(region_results)} recursos")
                    
                except Exception as e:
                    print(f"   ❌ {region} ({completed_count}/{len(regions)}): {str(e)[:50]}")
                    self.log_error(service_name, region, e)
        
        return results
    
    def list_ebs_volumes(self):
        """Listar volumes EBS - versão otimizada"""
        return self.parallel_region_query('EBS', self.get_ebs_volumes_by_region)

    def get_ebs_volumes_by_region(self, region):
        """Obter volumes EBS de uma região específica"""
        volumes = []
        try:
            ec2 = boto3.client('ec2', region_name=region, config=self.config)
            response = ec2.describe_volumes()
            
            for volume in response['Volumes']:
                if self.is_cancelled():
                    break
                    
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
            pass  # Erro já será logado pelo método pai
        
        return volumes
    
    def list_s3_buckets(self):
        """Listar buckets S3 com timeout otimizado e paginação controlada"""
        buckets = []
        try:
            s3 = boto3.client('s3', config=self.config)
            response = s3.list_buckets()
            
            print(f"🪣 Encontrados {len(response['Buckets'])} buckets S3")
            
            # Usar ThreadPoolExecutor para processar buckets em paralelo com timeout
            with ThreadPoolExecutor(max_workers=3) as executor:
                future_to_bucket = {}
                
                for bucket in response['Buckets']:
                    if self.is_cancelled():
                        break
                        
                    bucket_name = bucket['Name']
                    future = executor.submit(self.get_bucket_info_safe, bucket_name, bucket)
                    future_to_bucket[future] = bucket_name
                
                # Processar resultados com timeout
                for future in as_completed(future_to_bucket, timeout=300):  # 5 minutos total
                    if self.is_cancelled():
                        break
                        
                    try:
                        bucket_info = future.result(timeout=45)  # 45 segundos por bucket
                        if bucket_info:
                            buckets.append(bucket_info)
                    except Exception as e:
                        bucket_name = future_to_bucket[future]
                        print(f"❌ Timeout/Erro no bucket {bucket_name}: {str(e)[:100]}")
                        self.log_error('s3-bucket-timeout', bucket_name, e)
                        
                        # Adicionar bucket com informações básicas
                        buckets.append({
                            'BucketName': bucket_name,
                            'Region': 'unknown',
                            'CreationDate': bucket['CreationDate'].isoformat(),
                            'Size': 'Timeout - não calculado',
                            'ObjectCount': 'N/A',
                            'SizeBytes': 0
                        })
                        
        except Exception as e:
            print(f"❌ Erro geral ao listar buckets S3: {e}")
            self.log_error('s3', 'global', e)
        
        return buckets

    def get_bucket_info_safe(self, bucket_name, bucket_data):
        """Obter informações do bucket com timeout e fallbacks rápidos"""
        try:
            print(f"🔍 Analisando bucket: {bucket_name}")
            
            s3 = boto3.client('s3', config=self.config)
            
            # Obter região do bucket com timeout
            try:
                bucket_region = s3.get_bucket_location(Bucket=bucket_name)['LocationConstraint']
                if bucket_region is None:
                    bucket_region = 'us-east-1'
            except:
                bucket_region = 'unknown'
            
            # Estratégia rápida: tentar CloudWatch primeiro
            size_info = self.get_bucket_size_cloudwatch(bucket_name, bucket_region)
            
            # Se CloudWatch falhar, tentar listagem limitada
            if size_info['size_bytes'] == 0 and size_info['formatted_size'] == 'N/A':
                size_info = self.get_bucket_size_limited(bucket_name)
            
            return {
                'BucketName': bucket_name,
                'Region': bucket_region,
                'CreationDate': bucket_data['CreationDate'].isoformat(),
                'Size': size_info['formatted_size'],
                'ObjectCount': size_info['object_count'],
                'SizeBytes': size_info['size_bytes']
            }
            
        except Exception as e:
            print(f"⚠️ Erro ao processar bucket {bucket_name}: {str(e)[:100]}")
            self.log_error('s3-bucket-process', bucket_name, e)
            return None

    def get_bucket_size_cloudwatch(self, bucket_name, bucket_region):
        """Tentar obter tamanho via CloudWatch (mais rápido)"""
        size_info = {
            'size_bytes': 0,
            'object_count': 0,
            'formatted_size': 'N/A'
        }
        
        try:
            cw_region = 'us-east-1' if bucket_region == 'us-east-1' else bucket_region
            cw = boto3.client('cloudwatch', region_name=cw_region, config=self.config)
            
            # Métricas dos últimos 3 dias (mais rápido que 7 dias)
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=3)
            
            # Obter tamanho
            metric = cw.get_metric_statistics(
                Namespace='AWS/S3',
                MetricName='BucketSizeBytes',
                Dimensions=[
                    {'Name': 'BucketName', 'Value': bucket_name},
                    {'Name': 'StorageType', 'Value': 'StandardStorage'}
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=86400,
                Statistics=['Average']
            )
            
            if metric['Datapoints']:
                size_bytes = metric['Datapoints'][-1]['Average']
                size_info['size_bytes'] = size_bytes
                size_info['formatted_size'] = self.format_size(size_bytes)
                
                # Tentar obter contagem de objetos
                count_metric = cw.get_metric_statistics(
                    Namespace='AWS/S3',
                    MetricName='NumberOfObjects',
                    Dimensions=[
                        {'Name': 'BucketName', 'Value': bucket_name},
                        {'Name': 'StorageType', 'Value': 'AllStorageTypes'}
                    ],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=86400,
                    Statistics=['Average']
                )
                
                if count_metric['Datapoints']:
                    size_info['object_count'] = int(count_metric['Datapoints'][-1]['Average'])
                
                print(f"   ✅ CloudWatch {bucket_name}: {size_info['formatted_size']}")
            
        except Exception as e:
            print(f"   ⚠️ CloudWatch falhou para {bucket_name}: {str(e)[:50]}")
        
        return size_info

    def get_bucket_size_limited(self, bucket_name):
        """Obter tamanho via listagem limitada (fallback)"""
        size_info = {
            'size_bytes': 0,
            'object_count': 0,
            'formatted_size': 'N/A'
        }
        
        try:
            print(f"   📋 Listagem limitada para {bucket_name}...")
            s3 = boto3.client('s3', config=self.config)
            
            paginator = s3.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(
                Bucket=bucket_name,
                PaginationConfig={
                    'MaxItems': 1000,  # Limitar a 1000 objetos para evitar timeout
                    'PageSize': 100
                }
            )
            
            total_size = 0
            total_count = 0
            pages_processed = 0
            max_pages = 10  # Máximo 10 páginas para evitar timeout
            
            for page in page_iterator:
                if self.is_cancelled() or pages_processed >= max_pages:
                    size_info['formatted_size'] += " (limitado)"
                    break
                    
                if 'Contents' in page:
                    for obj in page['Contents']:
                        total_size += obj['Size']
                        total_count += 1
                
                pages_processed += 1
            
            size_info['size_bytes'] = total_size
            size_info['object_count'] = total_count
            size_info['formatted_size'] = self.format_size(total_size)
            
            if pages_processed >= max_pages:
                size_info['formatted_size'] += " (estimativa)"
            
            print(f"   ✅ Listagem {bucket_name}: {size_info['formatted_size']} ({total_count}+ objetos)")
            
        except Exception as e:
            print(f"   ❌ Listagem falhou para {bucket_name}: {str(e)[:50]}")
            size_info['formatted_size'] = "Erro ao calcular"
        
        return size_info
    
    def format_size(self, size_bytes):
        """Formatar tamanho em bytes para formato legível"""
        if size_bytes == 0:
            return "0 B (vazio)"
        elif size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024**2:
            return f"{size_bytes/1024:.2f} KB"
        elif size_bytes < 1024**3:
            return f"{size_bytes/(1024**2):.2f} MB"
        elif size_bytes < 1024**4:
            return f"{size_bytes/(1024**3):.2f} GB"
        else:
            return f"{size_bytes/(1024**4):.2f} TB"
    
    def list_rds_instances(self):
        """Listar instâncias RDS - versão otimizada"""
        return self.parallel_region_query('RDS', self.get_rds_instances_by_region)

    def get_rds_instances_by_region(self, region):
        """Obter instâncias RDS de uma região específica"""
        rds_instances = []
        try:
            rds = boto3.client('rds', region_name=region, config=self.config)
            
            # RDS Instances
            instances = rds.describe_db_instances()
            for instance in instances['DBInstances']:
                if self.is_cancelled():
                    break
                    
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
                    if self.is_cancelled():
                        break
                        
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
            pass  # Erro será logado pelo método pai
        
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
        """Listar distribuições CloudFront - versão otimizada"""
        distributions = []
        try:
            cf = boto3.client('cloudfront', config=self.config)
            response = cf.list_distributions()
            
            if 'DistributionList' in response and response['DistributionList']['Quantity'] > 0:
                for dist in response['DistributionList']['Items']:
                    if self.is_cancelled():
                        break
                        
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
        print("🚀 Iniciando inventário OTIMIZADO de recursos AWS...")
        print(f"⏱️ Timeout configurado: 30 minutos máximo")
        print(f"🌍 Processando até {len(self.get_all_regions())} regiões em paralelo")
        print("💡 Use Ctrl+C para cancelar a qualquer momento\n")
        
        max_runtime = 30 * 60  # 30 minutos máximo
        
        tasks = [
            ("EC2", self.list_ec2_instances),
            ("EBS", self.list_ebs_volumes), 
            ("S3", self.list_s3_buckets),
            ("RDS", self.list_rds_instances),
            ("Load Balancers", self.list_load_balancers_optimized),
            ("Lambda", self.list_lambda_functions_optimized),
            ("NAT Gateways", self.list_nat_gateways_optimized),
            ("Elastic IPs", self.list_elastic_ips_optimized),
            ("CloudFront", self.list_cloudfront_distributions)
        ]
        
        total_tasks = len(tasks)
        
        for i, (service_name, task_func) in enumerate(tasks, 1):
            if self.is_cancelled():
                print("⚠️ Inventário cancelado pelo usuário")
                break
                
            elapsed = self.get_elapsed_time()
            if elapsed > max_runtime:
                print(f"⏰ Timeout global atingido ({max_runtime//60} min). Finalizando...")
                break
            
            progress = f"[{i}/{total_tasks}]"
            print(f"\n{progress} Coletando {service_name}...")
            
            try:
                start_time = time.time()
                
                if service_name == "EC2":
                    self.inventory['EC2_Instances'] = task_func()
                elif service_name == "EBS":
                    self.inventory['EBS_Volumes'] = task_func()
                elif service_name == "S3":
                    self.inventory['S3_Buckets'] = task_func()
                elif service_name == "RDS":
                    self.inventory['RDS_Instances'] = task_func()
                elif service_name == "Load Balancers":
                    self.inventory['Load_Balancers'] = task_func()
                elif service_name == "Lambda":
                    self.inventory['Lambda_Functions'] = task_func()
                elif service_name == "NAT Gateways":
                    self.inventory['NAT_Gateways'] = task_func()
                elif service_name == "Elastic IPs":
                    self.inventory['Elastic_IPs'] = task_func()
                elif service_name == "CloudFront":
                    self.inventory['CloudFront_Distributions'] = task_func()
                
                duration = time.time() - start_time
                count = len(self.inventory.get(service_name.replace(' ', '_').replace(' ', ''), []))
                print(f"✅ {service_name}: {count} recursos em {duration:.1f}s")
                
            except Exception as e:
                print(f"❌ Erro ao coletar {service_name}: {str(e)[:100]}")
                self.log_error(service_name.lower(), 'global', e)
        
        # Recursos adicionais (se tempo permitir)
        elapsed = self.get_elapsed_time()
        if elapsed < max_runtime * 0.8 and not self.is_cancelled():  # Se usado menos de 80% do tempo
            print(f"\n[Extra] Coletando recursos adicionais...")
            try:
                additional = self.list_additional_cost_resources_optimized()
                self.inventory.update(additional)
            except Exception as e:
                print(f"⚠️ Erro nos recursos adicionais: {str(e)[:100]}")
        
        # Adicionar metadata final
        self.inventory['_metadata'] = {
            'generated_at': datetime.utcnow().isoformat(),
            'execution_time_seconds': round(self.get_elapsed_time(), 2),
            'total_errors': len(self.errors),
            'cancelled': self.cancelled,
            'summary': self._generate_summary()
        }
        
        if self.errors:
            self.inventory['_errors'] = self.errors
        
        return self.inventory

    def _generate_summary(self):
        """Gerar resumo dos recursos coletados"""
        summary = {}
        service_mapping = {
            'EC2_Instances': 'ec2_instances',
            'EBS_Volumes': 'ebs_volumes',
            'S3_Buckets': 's3_buckets',
            'RDS_Instances': 'rds_instances',
            'Load_Balancers': 'load_balancers',
            'Lambda_Functions': 'lambda_functions',
            'NAT_Gateways': 'nat_gateways',
            'Elastic_IPs': 'elastic_ips',
            'CloudFront_Distributions': 'cloudfront_distributions'
        }
        
        for key, summary_key in service_mapping.items():
            summary[summary_key] = len(self.inventory.get(key, []))
            
        # Adicionar recursos extras se existirem
        extra_services = [
            'EFS_FileSystems', 'ElastiCache_Clusters', 'Redshift_Clusters',
            'OpenSearch_Domains', 'API_Gateways', 'ECS_Clusters', 'EKS_Clusters'
        ]
        
        for service in extra_services:
            if service in self.inventory:
                summary[service.lower()] = len(self.inventory[service])
        
        return summary

    # Métodos otimizados para outros recursos
    def list_load_balancers_optimized(self):
        return self.parallel_region_query('Load Balancers', self.get_load_balancers_by_region, max_workers=2)

    def list_lambda_functions_optimized(self):
        return self.parallel_region_query('Lambda', self.get_lambda_functions_by_region, max_workers=2)

    def list_nat_gateways_optimized(self):
        return self.parallel_region_query('NAT Gateway', self.get_nat_gateways_by_region, max_workers=2)

    def get_load_balancers_by_region(self, region):
        """Obter Load Balancers de uma região específica"""
        load_balancers = []
        try:
            # ELBv2 (ALB/NLB)
            elbv2 = boto3.client('elbv2', region_name=region, config=self.config)
            lbs = elbv2.describe_load_balancers()
            
            for lb in lbs['LoadBalancers']:
                if self.is_cancelled():
                    break
                    
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
            elb = boto3.client('elb', region_name=region, config=self.config)
            classic_lbs = elb.describe_load_balancers()
            
            for clb in classic_lbs['LoadBalancerDescriptions']:
                if self.is_cancelled():
                    break
                    
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
            pass  # Erro será logado pelo método pai
        
        return load_balancers

    def get_lambda_functions_by_region(self, region):
        """Obter funções Lambda de uma região específica"""
        functions = []
        try:
            lambda_client = boto3.client('lambda', region_name=region, config=self.config)
            response = lambda_client.list_functions()
            
            for func in response['Functions']:
                if self.is_cancelled():
                    break
                    
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
            pass  # Erro será logado pelo método pai
        
        return functions

    def get_nat_gateways_by_region(self, region):
        """Obter NAT Gateways de uma região específica"""
        nat_gateways = []
        try:
            ec2 = boto3.client('ec2', region_name=region, config=self.config)
            response = ec2.describe_nat_gateways()
            
            for nat in response['NatGateways']:
                if self.is_cancelled():
                    break
                    
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
            pass  # Erro será logado pelo método pai
        
        return nat_gateways

    def get_elastic_ips_by_region(self, region):
        """Obter Elastic IPs de uma região específica"""
        eips = []
        try:
            ec2 = boto3.client('ec2', region_name=region, config=self.config)
            response = ec2.describe_addresses()
            
            for eip in response['Addresses']:
                if self.is_cancelled():
                    break
                    
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
            pass  # Erro será logado pelo método pai
        
        return eips

    def list_additional_cost_resources_optimized(self):
        """Listar recursos adicionais com timeout reduzido"""
        additional_resources = {}
        
        # Só coletar recursos extras se ainda temos tempo
        if self.get_elapsed_time() < 20 * 60:  # Menos de 20 minutos
            extra_tasks = [
                ("EFS", self.list_efs_filesystems_optimized, "EFS_FileSystems"),
                ("ElastiCache", self.list_elasticache_clusters_optimized, "ElastiCache_Clusters"),
            ]
            
            for service_name, task_func, key in extra_tasks:
                if self.is_cancelled() or self.get_elapsed_time() > 25 * 60:
                    break
                    
                try:
                    print(f"   Coletando {service_name}...")
                    additional_resources[key] = task_func()
                except Exception as e:
                    print(f"   ⚠️ {service_name}: {str(e)[:50]}")
        
        return additional_resources

    def list_efs_filesystems_optimized(self):
        return self.parallel_region_query('EFS', self.get_efs_by_region, max_workers=2, timeout=60)

    def list_elasticache_clusters_optimized(self):
        return self.parallel_region_query('ElastiCache', self.get_elasticache_by_region, max_workers=2, timeout=60)

    def get_efs_by_region(self, region):
        """Obter sistemas EFS de uma região específica"""
        filesystems = []
        try:
            efs = boto3.client('efs', region_name=region, config=self.config)
            response = efs.describe_file_systems()
            
            for fs in response['FileSystems']:
                if self.is_cancelled():
                    break
                    
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
            pass
        
        return filesystems

    def get_elasticache_by_region(self, region):
        """Obter clusters ElastiCache de uma região específica"""
        clusters = []
        try:
            ec = boto3.client('elasticache', region_name=region, config=self.config)
            
            # Redis clusters
            redis_clusters = ec.describe_cache_clusters()
            for cluster in redis_clusters['CacheClusters']:
                if self.is_cancelled():
                    break
                    
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
            try:
                repl_groups = ec.describe_replication_groups()
                for group in repl_groups['ReplicationGroups']:
                    if self.is_cancelled():
                        break
                        
                    clusters.append({
                        'Region': region,
                        'ReplicationGroupId': group['ReplicationGroupId'],
                        'Status': group['Status'],
                        'NodeType': group.get('CacheNodeType', 'N/A'),
                        'NumNodeGroups': len(group.get('NodeGroups', [])),
                        'Type': 'Replication Group'
                    })
            except:
                pass
                
        except Exception as e:
            pass
        
        return clusters

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
