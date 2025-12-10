import os

# --- AWS 리전 ---
SSM_REGION = os.environ.get('SSM_REGION', 'ap-northeast-2')
BEDROCK_REGION = os.environ.get('BEDROCK_REGION', 'ap-northeast-2')

# --- Bedrock 모델 ---
BEDROCK_MODEL_ID = os.environ.get('BEDROCK_MODEL_ID', 'amazon.titan-embed-text-v2:0')

# --- SSM 파라미터 경로 ---
OS_HOST_PARAM = os.environ.get('OS_HOST_PARAM', '/planit/os-host')
OS_USER_PARAM = os.environ.get('OS_USER_PARAM', '/planit/os-user')
OS_PASS_PARAM = os.environ.get('OS_PASS_PARAM', '/planit/os-pass')

ES_HOST_PARAM = os.environ.get('ES_HOST_PARAM', '/planit/llm/es-host')
ES_USER_PARAM = os.environ.get('ES_USER_PARAM', '/planit/es-user/super')
ES_PASS_PARAM = os.environ.get('ES_PASS_PARAM', '/planit/es-password/super')

# --- 인덱스 이름 ---
TARGET_INDEX = os.environ.get('TARGET_INDEX', 'edr-tickets-vector') # Vector DB (OpenSearch)
SOURCE_INDEX = os.environ.get('SOURCE_INDEX', 'ticket-result-*')    # 원본 DB (EC2 Elastic)