import os

# --- 공통 ---
AWS_REGION = os.environ.get("AWS_REGION", "ap-northeast-2")

# --- OpenAI (요약용 LLM) ---
OPENAI_SECRET_PARAM = os.environ.get("OPENAI_SECRET_PARAM", "/planit/llm/secret-name")
OPENAI_SECRET_NAME = os.environ.get("OPENAI_SECRET_NAME", "planit-openai-secret")
OPENAI_MODEL_ID    = os.environ.get("OPENAI_MODEL_ID", "gpt-5")   # 필요하면 gpt-4.1 등으로 변경

# --- Bedrock (임베딩용) ---
BEDROCK_EMBED_MODEL_ID = os.environ.get(
    "BEDROCK_EMBED_MODEL_ID",
    "amazon.titan-embed-text-v2:0"
)

# --- OpenSearch (벡터 DB) ---
# SSM Parameter Store에 저장된 값의 이름
SSM_OS_HOST_PARAM = os.environ.get("OS_HOST_PARAM", "/planit/os-host")
SSM_OS_USER_PARAM = os.environ.get("OS_USER_PARAM", "/planit/os-user")
SSM_OS_PASS_PARAM = os.environ.get("OS_PASS_PARAM", "/planit/os-pass")

# 실제 인덱스 이름
OS_INDEX = os.environ.get("OS_INDEX", "edr-event-vectors")

# --- ElasticSearch ---
SSM_ES_HOST_PARAM = os.environ.get("ES_HOST_PARAM", "/planit/llm/es-host")
SSM_ES_USER_PARAM = os.environ.get("ES_USER_PARAM", "/planit/es-user/super") 
SSM_ES_PASS_PARAM = os.environ.get("ES_PASSWORD_PARAM", "/planit/es-password/super")
