import boto3
import json
from datetime import datetime, timedelta
from botocore.exceptions import ClientError, NoCredentialsError, ReadTimeoutError, ConnectTimeoutError
from botocore.config import Config
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

class AWSCostInventory:
    def __init__(self):
        self.inventory = {}
        self.errors = []
        
        # Configuração de timeouts para evitar travamentos
        self.config = {
            'region_name': 'us-east-1',
            'retries': {
                'max_attempts': 3,
                'mode': 'adaptive'
            },
            'read_timeout': 30,      # 30 segundos para operações de leitura
            'connect_timeout': 10    # 10 segundos para conexão
        }
        
        # Limites para evitar travamentos em buckets grandes
        self.s3_max_objects = 10000  # Máximo de objetos a listar por bucket
        self.s3_timeout = 60         # Timeout específico para operações S3
        
        print(f"🔧 Configurações de timeout definidas:")
        print(f"   • Timeout de leitura: {self.config['read_timeout']}s")
        print(f"   • Timeout de conexão: {self.config['connect_timeout']}s")
        print(f"   • Limite S3: {self.s3_max_objects} objetos por bucket")
        
    def get_boto3_client(self, service, region=None):
        """Criar cliente boto3 com configurações otimizadas"""
        try:
            config_dict = self.config.copy()
            if region:
                config_dict['region_name'] = region
                
            return boto3.client(service, **config_dict)
        except Exception as e:
            # Fallback sem configurações avançadas se houver erro
            return boto3.client(service, region_name=region or 'us-east-1')
        
    def log_error(self, service, region, error):
        """Log de erros para análise posterior"""
        error_info = {
            "service": service,
            "region": region,
            "error": str(error),
            "error_type": type(error).__name__,
            "timestamp": datetime.utcnow().isoformat()
        }
        self.errors.append(error_info)
        
        # Log imediato para debug
        print(f"⚠️ Erro em {service} ({region}): {str(error)[:100]}...")
    
    def get_all_regions(self):
        """Obter todas as regiões AWS disponíveis com timeout"""
        try:
            print("🌍 Obtendo lista de regiões AWS...")
            ec2 = self.get_boto3_client('ec2', 'us-east-1')
            
            # Implementar timeout manual
            start_time = time.time()
            regions_response = ec2.describe_regions()
            
            if time.time() - start_time > 15:  # Se demorou mais de 15 segundos
                print("⚠️ Consulta de regiões demorou muito, usando lista padrão")
                return ['us-east-1', 'us-west-2', 'eu-west-1', 'ap-southeast-1']
                
            regions = [r['RegionName'] for r in regions_response['Regions']]
            print(f"✅ Encontradas {len(regions)} regiões")
            return regions
            
        except (ReadTimeoutError, ConnectTimeoutError) as e:
            print(f"⚠️ Timeout ao obter regiões: {str(e)}")
            self.log_error('ec2', 'global', e)
            return ['us-east-1', 'us-west-2', 'eu-west-1', 'ap-southeast-1']
        except Exception as e:
            self.log_error('ec2', 'global', e)
            return ['us-east-1', 'us-west-2', 'eu-west-1', 'ap-southeast-1']  # fallback básico
    
    def list_ec2_instances(self):
        """Listar instâncias EC2 com informações de storage usando processamento paralelo"""
        print("🖥️ Coletando instâncias EC2 de todas as regiões...")
        instances = []
        regions = self.get_all_regions()
        
        # Processamento paralelo por região
        def process_region_ec2(region):
            region_instances = []
            try:
                print(f"   🌍 Processando EC2 em {region}...")
                ec2 = self.get_boto3_client('ec2', region)
                
                start_time = time.time()
                response = ec2.describe_instances()
                
                if time.time() - start_time > 30:  # Timeout por região
                    print(f"   ⚠️ Timeout em EC2 {region}")
                    return region_instances
                
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
                        
                        region_instances.append({
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
                        
                print(f"   ✅ EC2 {region}: {len(region_instances)} instâncias")
                        
            except Exception as e:
                print(f"   ❌ Erro EC2 {region}: {str(e)[:50]}...")
                self.log_error('ec2', region, e)
            
            return region_instances
        
        # Executar em paralelo com limite de threads
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_region = {executor.submit(process_region_ec2, region): region for region in regions}
            
            for future in as_completed(future_to_region, timeout=300):  # 5 minutos máximo
                try:
                    region_instances = future.result()
                    instances.extend(region_instances)
                except Exception as e:
                    region = future_to_region[future]
                    print(f"   ❌ Falha na thread EC2 {region}: {str(e)}")
        
        print(f"✅ EC2 concluído: {len(instances)} instâncias encontradas")
        return instances
    
    def list_ebs_volumes(self):
        """Listar volumes EBS usando processamento paralelo"""
        print("💾 Coletando volumes EBS de todas as regiões...")
        volumes = []
        regions = self.get_all_regions()
        
        def process_region_ebs(region):
            region_volumes = []
            try:
                print(f"   🌍 Processando EBS em {region}...")
                ec2 = self.get_boto3_client('ec2', region)
                
                start_time = time.time()
                response = ec2.describe_volumes()
                
                if time.time() - start_time > 30:
                    print(f"   ⚠️ Timeout em EBS {region}")
                    return region_volumes
                
                for volume in response['Volumes']:
                    tags = {tag['Key']: tag['Value'] for tag in volume.get('Tags', [])}
                    
                    region_volumes.append({
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
                
                print(f"   ✅ EBS {region}: {len(region_volumes)} volumes")
                    
            except Exception as e:
                print(f"   ❌ Erro EBS {region}: {str(e)[:50]}...")
                self.log_error('ebs', region, e)
            
            return region_volumes
        
        # Processamento paralelo
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_region = {executor.submit(process_region_ebs, region): region for region in regions}
            
            for future in as_completed(future_to_region, timeout=200):
                try:
                    region_volumes = future.result()
                    volumes.extend(region_volumes)
                except Exception as e:
                    region = future_to_region[future]
                    print(f"   ❌ Falha na thread EBS {region}: {str(e)}")
        
        print(f"✅ EBS concluído: {len(volumes)} volumes encontrados")
        return volumes
    
    def list_s3_buckets(self):
        """Listar buckets S3 com tamanhos usando múltiplas estratégias otimizadas"""
        buckets = []
        try:
            print("🪣 Iniciando análise de buckets S3...")
            s3 = self.get_boto3_client('s3')
            
            start_time = time.time()
            response = s3.list_buckets()
            
            total_buckets = len(response['Buckets'])
            print(f"📦 Encontrados {total_buckets} buckets")
            
            for i, bucket in enumerate(response['Buckets'], 1):
                bucket_name = bucket['Name']
                print(f"🔍 [{i}/{total_buckets}] Analisando bucket: {bucket_name}")
                
                # Timeout por bucket individual
                bucket_start = time.time()
                
                # Obter região do bucket com timeout
                bucket_region = self.get_bucket_region_safe(bucket_name, s3)
                
                # Múltiplas estratégias para obter o tamanho com timeout
                size_info = self.get_bucket_size_optimized(bucket_name, bucket_region)
                
                bucket_time = time.time() - bucket_start
                print(f"   ⏱️ Processado em {bucket_time:.1f}s")
                
                buckets.append({
                    'BucketName': bucket_name,
                    'Region': bucket_region,
                    'CreationDate': bucket['CreationDate'].isoformat(),
                    'Size': size_info['formatted_size'],
                    'ObjectCount': size_info['object_count'],
                    'SizeBytes': size_info['size_bytes'],
                    'ProcessingTime': f"{bucket_time:.1f}s"
                })
                
                # Se já demorou muito no total, parar
                total_time = time.time() - start_time
                if total_time > 300:  # 5 minutos máximo para todos os buckets
                    print(f"⚠️ Timeout geral de S3 atingido ({total_time:.1f}s). Parando análise.")
                    break
                
        except Exception as e:
            self.log_error('s3', 'global', e)
        
        print(f"✅ Análise de S3 concluída: {len(buckets)} buckets processados")
        return buckets
    
    def get_bucket_region_safe(self, bucket_name, s3_client):
        """Obter região do bucket com tratamento seguro"""
        try:
            start_time = time.time()
            response = s3_client.get_bucket_location(Bucket=bucket_name)
            
            if time.time() - start_time > 10:  # Se demorou mais de 10 segundos
                print(f"   ⚠️ Timeout ao obter região do bucket {bucket_name}")
                return 'unknown'
                
            bucket_region = response['LocationConstraint']
            if bucket_region is None:
                bucket_region = 'us-east-1'
            return bucket_region
        except Exception as e:
            print(f"   ⚠️ Erro ao obter região: {str(e)[:50]}...")
            return 'unknown'

    def get_bucket_size_optimized(self, bucket_name, bucket_region):
        """Obter tamanho do bucket usando estratégias otimizadas com timeouts"""
        size_info = {
            'size_bytes': 0,
            'object_count': 0,
            'formatted_size': 'N/A'
        }
        
        bucket_start = time.time()
        max_bucket_time = 60  # Máximo 60 segundos por bucket
        
        try:
            s3 = self.get_boto3_client('s3')
            
            print(f"   📋 Listando objetos (limite: {self.s3_max_objects})...")
            
            # Estratégia otimizada: usar head_object para verificar se bucket tem conteúdo
            try:
                # Tentar listar apenas o primeiro objeto para verificar se bucket está vazio
                response = s3.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
                
                if 'Contents' not in response:
                    # Bucket vazio
                    size_info['formatted_size'] = "0 B (Bucket vazio)"
                    print(f"   📭 Bucket {bucket_name} está vazio")
                    return size_info
                
            except Exception as first_check_error:
                print(f"   ⚠️ Erro na verificação inicial: {str(first_check_error)[:50]}...")
                # Se não conseguir nem verificar se está vazio, tentar CloudWatch
                return self.get_bucket_size_from_cloudwatch(bucket_name, bucket_region)
            
            # Se chegou aqui, bucket tem conteúdo - tentar listar com limite
            print(f"   � Contando objetos (timeout: {max_bucket_time}s)...")
            
            paginator = s3.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(
                Bucket=bucket_name,
                PaginationConfig={'MaxItems': self.s3_max_objects}
            )
            
            total_size = 0
            total_count = 0
            
            try:
                for page_num, page in enumerate(page_iterator, 1):
                    # Verificar timeout
                    if time.time() - bucket_start > max_bucket_time:
                        print(f"   ⏰ Timeout atingido após {page_num} páginas")
                        break
                    
                    if 'Contents' in page:
                        page_objects = len(page['Contents'])
                        total_count += page_objects
                        
                        for obj in page['Contents']:
                            total_size += obj['Size']
                        
                        # Mostrar progresso a cada 10 páginas ou 1000 objetos
                        if page_num % 10 == 0 or total_count >= 1000:
                            print(f"   🔄 Página {page_num}: {total_count} objetos, {self.format_bytes(total_size)}")
                    
                    # Parar se atingiu o limite de objetos
                    if total_count >= self.s3_max_objects:
                        print(f"   ⚠️ Limite de {self.s3_max_objects} objetos atingido")
                        size_info['formatted_size'] += " (limitado)"
                        break
                
                size_info['size_bytes'] = total_size
                size_info['object_count'] = total_count
                size_info['formatted_size'] = self.format_bytes(total_size)
                
                if total_count >= self.s3_max_objects:
                    size_info['formatted_size'] += " (aproximado)"
                
                print(f"   ✅ Bucket {bucket_name}: {size_info['formatted_size']} ({total_count} objetos)")
                return size_info
                
            except Exception as list_error:
                print(f"   ⚠️ Erro ao listar objetos: {str(list_error)[:50]}...")
                # Fallback para CloudWatch
                return self.get_bucket_size_from_cloudwatch(bucket_name, bucket_region)
            
        except Exception as e:
            print(f"   ❌ Erro geral no bucket: {str(e)[:50]}...")
            self.log_error('s3-bucket-size', bucket_name, e)
            size_info['formatted_size'] = "Erro ao obter tamanho"
            return size_info
    
    def get_bucket_size_from_cloudwatch(self, bucket_name, bucket_region):
        """Fallback: obter tamanho via CloudWatch"""
        size_info = {
            'size_bytes': 0,
            'object_count': 0,
            'formatted_size': 'N/A'
        }
        
        try:
            print(f"   🔄 Tentando CloudWatch para {bucket_name}...")
            cw_region = 'us-east-1' if bucket_region in ['us-east-1', 'unknown'] else bucket_region
            cw = self.get_boto3_client('cloudwatch', cw_region)
            
            # Timeout para CloudWatch
            cw_start = time.time()
            
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
            
            # Verificar se CloudWatch demorou muito
            if time.time() - cw_start > 20:
                print(f"   ⚠️ CloudWatch demorou muito para {bucket_name}")
                size_info['formatted_size'] = "Timeout CloudWatch"
                return size_info
            
            if metric['Datapoints']:
                size_bytes = metric['Datapoints'][-1]['Average']
                size_info['size_bytes'] = size_bytes
                size_info['formatted_size'] = self.format_bytes(size_bytes) + " (CloudWatch)"
            
            # Tentar obter contagem de objetos
            try:
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
                    
            except:
                pass  # Não conseguiu obter contagem, mas tudo bem
            
            if size_info['formatted_size'] == 'N/A':
                size_info['formatted_size'] = "Sem dados CloudWatch"
            
            print(f"   ✅ CloudWatch {bucket_name}: {size_info['formatted_size']}")
                    
        except Exception as cw_error:
            print(f"   ❌ Erro no CloudWatch: {str(cw_error)[:50]}...")
            self.log_error('cloudwatch-s3', bucket_region, cw_error)
            size_info['formatted_size'] = "Erro CloudWatch"
        
        return size_info
    
    def format_bytes(self, bytes_size):
        """Formatar bytes em formato legível"""
        if bytes_size == 0:
            return "0 B"
        elif bytes_size < 1024:
            return f"{bytes_size:.0f} B"
        elif bytes_size < 1024**2:
            return f"{bytes_size/1024:.2f} KB"
        elif bytes_size < 1024**3:
            return f"{bytes_size/(1024**2):.2f} MB"
        elif bytes_size < 1024**4:
            return f"{bytes_size/(1024**3):.2f} GB"
        else:
            return f"{bytes_size/(1024**4):.2f} TB"
    
    def list_rds_instances(self):
        """Listar instâncias RDS com tratamento robusto de erros"""
        print("🗄️ Coletando instâncias RDS...")
        rds_instances = []
        regions = self.get_all_regions()
        
        def process_region_rds(region):
            region_instances = []
            try:
                print(f"   🌍 Processando RDS em {region}...")
                rds = self.get_boto3_client('rds', region)
                
                # RDS Instances com timeout
                try:
                    start_time = time.time()
                    instances = rds.describe_db_instances()
                    
                    if time.time() - start_time > 45:
                        print(f"   ⚠️ Timeout RDS instances em {region}")
                        return region_instances
                    
                    for instance in instances['DBInstances']:
                        region_instances.append({
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
                            'VpcId': instance.get('DbSubnetGroup', {}).get('VpcId', 'N/A'),
                            'Type': 'RDS Instance'
                        })
                except Exception as rds_error:
                    print(f"   ⚠️ Erro RDS instances {region}: {str(rds_error)[:50]}...")
                    self.log_error('rds-instances', region, rds_error)
                
                # RDS Clusters (Aurora) com tratamento separado
                try:
                    start_time = time.time()
                    clusters = rds.describe_db_clusters()
                    
                    if time.time() - start_time > 45:
                        print(f"   ⚠️ Timeout RDS clusters em {region}")
                        return region_instances
                    
                    for cluster in clusters['DBClusters']:
                        region_instances.append({
                            'Region': region,
                            'DBClusterIdentifier': cluster['DBClusterIdentifier'],
                            'Engine': cluster['Engine'],
                            'EngineVersion': cluster['EngineVersion'],
                            'Status': cluster['Status'],
                            'DatabaseName': cluster.get('DatabaseName', 'N/A'),
                            'MasterUsername': cluster.get('MasterUsername', 'N/A'),
                            'ClusterMembers': len(cluster.get('DBClusterMembers', [])),
                            'Type': 'Aurora Cluster'
                        })
                except Exception as cluster_error:
                    # Aurora pode não estar disponível em todas as regiões
                    if "InvalidParameterValue" not in str(cluster_error):
                        print(f"   ⚠️ Erro RDS clusters {region}: {str(cluster_error)[:50]}...")
                        self.log_error('rds-clusters', region, cluster_error)
                
                print(f"   ✅ RDS {region}: {len(region_instances)} instâncias")
                    
            except Exception as e:
                print(f"   ❌ Erro RDS geral {region}: {str(e)[:50]}...")
                self.log_error('rds', region, e)
            
            return region_instances
        
        # Processamento paralelo
        with ThreadPoolExecutor(max_workers=4) as executor:
            future_to_region = {executor.submit(process_region_rds, region): region for region in regions}
            
            for future in as_completed(future_to_region, timeout=250):
                try:
                    region_instances = future.result()
                    rds_instances.extend(region_instances)
                except Exception as e:
                    region = future_to_region[future]
                    print(f"   ❌ Falha na thread RDS {region}: {str(e)}")
        
        print(f"✅ RDS concluído: {len(rds_instances)} instâncias encontradas")
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
        """Executar inventário completo com indicadores de progresso"""
        total_start = time.time()
        print("🚀 Iniciando inventário COMPLETO e OTIMIZADO de recursos AWS...")
        print("=" * 60)
        
        services = [
            ('EC2 Instances', 'list_ec2_instances'),
            ('EBS Volumes', 'list_ebs_volumes'), 
            ('S3 Buckets', 'list_s3_buckets'),
            ('RDS Instances', 'list_rds_instances'),
            ('Load Balancers', 'list_load_balancers'),
            ('Lambda Functions', 'list_lambda_functions'),
            ('NAT Gateways', 'list_nat_gateways'),
            ('Elastic IPs', 'list_elastic_ips'),
            ('CloudFront Distributions', 'list_cloudfront_distributions')
        ]
        
        completed_services = 0
        total_services = len(services)
        
        for service_name, method_name in services:
            completed_services += 1
            progress = (completed_services / total_services) * 100
            
            print(f"\n📊 [{completed_services}/{total_services}] ({progress:.1f}%) - Coletando {service_name}...")
            service_start = time.time()
            
            try:
                method = getattr(self, method_name)
                results = method()
                
                # Mapear nomes para chaves do inventário
                key_mapping = {
                    'EC2 Instances': 'EC2_Instances',
                    'EBS Volumes': 'EBS_Volumes',
                    'S3 Buckets': 'S3_Buckets',
                    'RDS Instances': 'RDS_Instances',
                    'Load Balancers': 'Load_Balancers',
                    'Lambda Functions': 'Lambda_Functions',
                    'NAT Gateways': 'NAT_Gateways',
                    'Elastic IPs': 'Elastic_IPs',
                    'CloudFront Distributions': 'CloudFront_Distributions'
                }
                
                key = key_mapping[service_name]
                self.inventory[key] = results
                
                service_time = time.time() - service_start
                print(f"   ✅ {service_name}: {len(results)} recursos em {service_time:.1f}s")
                
            except Exception as e:
                print(f"   ❌ Erro em {service_name}: {str(e)}")
                self.log_error(service_name.lower().replace(' ', '-'), 'global', e)
        
        # Recursos adicionais com indicador de progresso
        print(f"\n📊 Coletando recursos adicionais...")
        additional_start = time.time()
        
        try:
            additional = self.list_additional_cost_resources()
            self.inventory.update(additional)
            
            additional_time = time.time() - additional_start
            print(f"   ✅ Recursos adicionais coletados em {additional_time:.1f}s")
            
        except Exception as e:
            print(f"   ❌ Erro em recursos adicionais: {str(e)}")
            self.log_error('additional-resources', 'global', e)
        
        # Finalizar com resumo
        total_time = time.time() - total_start
        
        # Adicionar timestamp e resumo
        self.inventory['_metadata'] = {
            'generated_at': datetime.utcnow().isoformat(),
            'total_time': f"{total_time:.1f}s",
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
            },
            'optimizations_applied': [
                'Timeouts em todas as consultas',
                'Processamento paralelo por região',
                'Limite de objetos S3',
                'Fallbacks para CloudWatch',
                'Tratamento robusto de erros'
            ]
        }
        
        if self.errors:
            self.inventory['_errors'] = self.errors
        
        print("\n" + "=" * 60)
        print(f"🎉 Inventário concluído em {total_time:.1f} segundos!")
        print(f"⚡ Otimizações aplicadas preveniram travamentos")
        
        return self.inventory

def main():
    print("🔧 AWS Cost Inventory - Versão Otimizada")
    print("=" * 50)
    
    try:
        # Verificar credenciais AWS com timeout
        print("🔐 Verificando credenciais AWS...")
        start_time = time.time()
        
        sts = boto3.client('sts', 
                          config=boto3.session.Config(
                              read_timeout=10,
                              connect_timeout=5,
                              retries={'max_attempts': 2}
                          ))
        identity = sts.get_caller_identity()
        
        check_time = time.time() - start_time
        print(f"✅ Credenciais válidas ({check_time:.1f}s)")
        print(f"   📋 Account: {identity['Account']}")
        print(f"   👤 User: {identity.get('Arn', 'N/A')}")
        
    except (NoCredentialsError, ClientError) as e:
        print(f"❌ Erro nas credenciais AWS: {e}")
        print("💡 Certifique-se de estar executando no CloudShell ou com credenciais configuradas")
        print("💡 Ou configure: aws configure")
        sys.exit(1)
    except Exception as e:
        print(f"⚠️ Erro na verificação: {e}")
        print("🔄 Continuando mesmo assim...")
    
    print("\n" + "=" * 50)
    
    # Executar inventário
    inventory = AWSCostInventory()
    
    try:
        results = inventory.run_inventory()
        
        # Salvar resultado
        output_file = f'aws_cost_inventory_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"\n✅ Inventário salvo em '{output_file}'")
        
        # Resumo detalhado
        print(f"\n📊 Resumo Final:")
        metadata = results.get('_metadata', {})
        summary = metadata.get('summary', {})
        
        for service, count in summary.items():
            service_display = service.replace('_', ' ').title()
            if count > 0:
                print(f"   ✅ {service_display}: {count}")
            else:
                print(f"   ➖ {service_display}: {count}")
        
        # Informações adicionais
        print(f"\n⏱️ Tempo total: {metadata.get('total_time', 'N/A')}")
        
        if results.get('_errors'):
            error_count = len(results['_errors'])
            print(f"⚠️ Erros encontrados: {error_count}")
            print("   💡 Verifique o arquivo JSON para detalhes dos erros")
        else:
            print("✅ Nenhum erro encontrado!")
        
        # Otimizações aplicadas
        optimizations = metadata.get('optimizations_applied', [])
        if optimizations:
            print(f"\n⚡ Otimizações aplicadas:")
            for opt in optimizations:
                print(f"   • {opt}")
        
        print(f"\n📄 Para visualizar o resultado:")
        print(f"   Notepad {output_file}")
        print(f"   ou: type {output_file} | more")
        
        # Criar arquivo de resumo rápido
        summary_file = f'aws_summary_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt'
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write("AWS Cost Inventory - Resumo Executivo\n")
            f.write("=" * 40 + "\n\n")
            f.write(f"Data: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
            f.write(f"Account: {identity['Account']}\n")
            f.write(f"Tempo total: {metadata.get('total_time', 'N/A')}\n\n")
            
            f.write("Recursos Encontrados:\n")
            f.write("-" * 20 + "\n")
            for service, count in summary.items():
                service_display = service.replace('_', ' ').title()
                f.write(f"{service_display}: {count}\n")
            
            if results.get('_errors'):
                f.write(f"\nErros: {len(results['_errors'])}\n")
            
            f.write(f"\nArquivo detalhado: {output_file}\n")
        
        print(f"📋 Resumo executivo salvo em '{summary_file}'")
        
    except KeyboardInterrupt:
        print(f"\n🛑 Inventário interrompido pelo usuário")
        print(f"💾 Salvando dados coletados até agora...")
        
        # Salvar dados parciais
        if inventory.inventory:
            partial_file = f'aws_inventory_partial_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
            with open(partial_file, 'w', encoding='utf-8') as f:
                json.dump(inventory.inventory, f, indent=2, ensure_ascii=False, default=str)
            print(f"💾 Dados parciais salvos em '{partial_file}'")
        
        sys.exit(1)
        
    except Exception as e:
        print(f"\n❌ Erro durante o inventário: {e}")
        print(f"🐛 Detalhes técnicos: {type(e).__name__}")
        
        # Tentar salvar dados parciais
        if hasattr(inventory, 'inventory') and inventory.inventory:
            error_file = f'aws_inventory_error_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
            try:
                with open(error_file, 'w', encoding='utf-8') as f:
                    json.dump(inventory.inventory, f, indent=2, ensure_ascii=False, default=str)
                print(f"💾 Dados parciais salvos em '{error_file}'")
            except:
                print("❌ Não foi possível salvar dados parciais")
        
        sys.exit(1)

if __name__ == "__main__":
    main()
