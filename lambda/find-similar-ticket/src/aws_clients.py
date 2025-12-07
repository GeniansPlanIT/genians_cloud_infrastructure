import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from elasticsearch import Elasticsearch
import config 


os_client = None
es_read_client = None
bedrock_client = None

def initialize_clients():
    """Lambda 실행 시 최초 1회 클라이언트를 초기화"""
    global os_client, es_read_client, bedrock_client
    
    # 이미 초기화되었다면 건너뛰기
    if os_client and es_read_client and bedrock_client:
        return

    try:
        print("SSM 파라미터 로드 중...")
        ssm_client = boto3.client('ssm', region_name=config.SSM_REGION)
        
        # 1. OpenSearch (Vector) 정보
        OS_HOST = ssm_client.get_parameter(Name=config.OS_HOST_PARAM)['Parameter']['Value']
        OS_USER = ssm_client.get_parameter(Name=config.OS_USER_PARAM)['Parameter']['Value']
        OS_PASSWORD = ssm_client.get_parameter(Name=config.OS_PASS_PARAM, WithDecryption=True)['Parameter']['Value']

        # 2. EC2 Elastic (7.x) 정보
        ES_HOST = ssm_client.get_parameter(Name=config.ES_HOST_PARAM)['Parameter']['Value']
        ES_USER = ssm_client.get_parameter(Name=config.ES_USER_PARAM)['Parameter']['Value']
        ES_PASSWORD = ssm_client.get_parameter(Name=config.ES_PASS_PARAM, WithDecryption=True)['Parameter']['Value']

        print("클라이언트 연결 시도...")
        # 3. Bedrock 클라이언트
        bedrock_client = boto3.client('bedrock-runtime', region_name=config.BEDROCK_REGION)

        # 4. OpenSearch (Vector) 클라이언트
        os_client = OpenSearch(
            hosts=[{'host': OS_HOST, 'port': 443}],
            http_auth=(OS_USER, OS_PASSWORD),
            use_ssl=True, verify_certs=True, ssl_assert_hostname=False, ssl_show_warn=False,
            connection_class=RequestsHttpConnection
        )
        
        # 5. EC2 Elastic (7.x) 클라이언트
        es_read_client = Elasticsearch(
            ES_HOST,
            http_auth=(ES_USER, ES_PASSWORD),
            request_timeout=30,
            sniff_on_start=False
        )
        print("모든 클라이언트 초기화 성공.")

    except Exception as e:
        print(f"클라이언트 초기화 실패: {e}")
        raise e # 핸들러가 오류를 잡도록 예외 발생