import json
import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
import config

_s3_client = None
_bedrock_client = None
_secrets_client = None
_openai_client = None
_ssm_client = None
_os_client = None


def get_s3_client():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client("s3", region_name=config.AWS_REGION)
    return _s3_client


def get_bedrock_client():
    global _bedrock_client
    if _bedrock_client is None:
        _bedrock_client = boto3.client("bedrock-runtime", region_name=config.AWS_REGION)
    return _bedrock_client


def get_secrets_client():
    global _secrets_client
    if _secrets_client is None:
        _secrets_client = boto3.client("secretsmanager", region_name=config.AWS_REGION)
    return _secrets_client


def get_ssm_client():
    global _ssm_client
    if _ssm_client is None:
        _ssm_client = boto3.client("ssm", region_name=config.AWS_REGION)
    return _ssm_client


def get_openai_client():
    """
    Secrets Manager에서 OPENAI_API_KEY를 가져와 OpenAI 클라이언트 생성
    """
    global _openai_client
    if _openai_client is not None:
        return _openai_client

    from openai import OpenAI

    try:
        ssm = get_ssm_client()
        secrets = get_secrets_client()


        # SSM에서 Secret 이름 가져오기
        print(f"SSM에서 OpenAI Secret 이름 로드 중... ({config.OPENAI_SECRET_PARAM})")
        param = ssm.get_parameter(Name=config.OPENAI_SECRET_PARAM, WithDecryption=False)
        secret_name = param["Parameter"]["Value"]

        # Secrets Manager에서 API Key 가져오기
        print(f"SecretsManager에서 OpenAI Secret({secret_name}) 조회 중...")
        sec = secrets.get_secret_value(SecretId=secret_name)
        payload = json.loads(sec["SecretString"])
        api_key = payload["OPENAI_API_KEY"]

        _openai_client = OpenAI(api_key=api_key)
        print("OpenAI 클라이언트 초기화 완료.")
        return _openai_client

    except Exception as e:
        print(f"OpenAI 클라이언트 초기화 실패: {e}")
        raise e


def get_opensearch_client():
    """
    SSM Parameter Store에서 OpenSearch 호스트/계정 정보를 읽어와 클라이언트 생성
    """
    global _os_client
    if _os_client is not None:
        return _os_client

    try:
        ssm = get_ssm_client()
        print("SSM에서 OpenSearch 접속 정보 로드 중...")

        host = ssm.get_parameter(Name=config.SSM_OS_HOST_PARAM)["Parameter"]["Value"]
        user = ssm.get_parameter(Name=config.SSM_OS_USER_PARAM)["Parameter"]["Value"]
        password = ssm.get_parameter(Name=config.SSM_OS_PASS_PARAM, WithDecryption=True)["Parameter"]["Value"]


        _os_client = OpenSearch(
            hosts=[{"host": host, "port": 443}],
            http_auth=(user, password),
            use_ssl=True,
            verify_certs=True,
            ssl_assert_hostname=False,
            ssl_show_warn=False,
            connection_class=RequestsHttpConnection,
        )
        print("OpenSearch 클라이언트 초기화 완료.")
        return _os_client

    except Exception as e:
        print(f"OpenSearch 클라이언트 초기화 실패: {e}")
        raise e
