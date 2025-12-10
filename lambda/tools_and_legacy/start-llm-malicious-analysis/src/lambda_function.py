import os
import json
import boto3
import logging
from datetime import datetime, timedelta, timezone
from elasticsearch import Elasticsearch
from requests.auth import HTTPBasicAuth

# ---- 로깅 및 전역 클라이언트 설정 ----
logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())

ssm = boto3.client('ssm')
sqs = boto3.client('sqs')

es_client = None

# ---- 헬퍼 함수 ----
def get_parameter(ssm_client, name, with_decryption=True):
    try:
        response = ssm_client.get_parameter(Name=name, WithDecryption=with_decryption)
        return response['Parameter']['Value']
    except Exception as e:
        logger.error(f"Failed to get SSM parameter '{name}': {e}")
        raise

# Elasticsearch 클라이언트 연결을 관리하는 함수
def get_es_client():
    """
    Elasticsearch 클라이언트의 연결 상태를 확인하고,
    유효한 클라이언트 객체를 반환합니다.
    - 클라이언트가 없거나(cold start) 연결이 끊겼으면(warm start) 새로 생성합니다.
    """
    global es_client

    # 클라이언트가 없거나, ping 테스트에 실패하면 새로 연결합니다.
    if es_client is None or not es_client.ping():
        logger.info("Initializing or re-initializing Elasticsearch client.")
        try:
            ES_HOST = get_parameter(ssm, '/planit/llm/es-host')
            ES_USER = get_parameter(ssm, '/planit/es-user/super')
            ES_PASSWORD = get_parameter(ssm, '/planit/es-password/super')

            es_client = Elasticsearch(
                ES_HOST,
                http_auth=(ES_USER, ES_PASSWORD),
                request_timeout=30,
                max_retries=5,
                retry_on_timeout=True
            )
            # 새로 생성한 후에도 연결이 잘 되었는지 최종 확인
            if not es_client.ping():
                logger.error("Failed to connect to Elasticsearch after initialization.")
                return None
            logger.info("Elasticsearch client is active and connected.")
            
        except Exception as e:
            logger.error(f"Error during Elasticsearch client initialization: {e}")
            es_client = None # 실패 시 클라이언트를 None으로 설정
            return None
    
    return es_client

def get_all_ids_from_index(current_es_client, index_name, page_size=1000):
    """Search + Pagination 방식으로 모든 문서 ID 조회"""
    ids = set()
    try:
        # 초기 검색
        resp = current_es_client.search(
            index=index_name,
            query={"match_all": {}},
            _source=False,
            size=page_size,
        )
        ids.update(hit['_id'] for hit in resp['hits']['hits'])

        total_hits = resp['hits']['total']['value'] if 'value' in resp['hits']['total'] else resp['hits']['total']
        retrieved = len(resp['hits']['hits'])
        scroll_from = retrieved

        # 반복 조회!
        while retrieved < total_hits:
            resp = current_es_client.search(
                index=index_name,
                query={"match_all": {}},
                _source=False,
                size=page_size,
                from_=scroll_from
            )
            batch_count = len(resp['hits']['hits'])
            if batch_count == 0:
                break
            ids.update(hit['_id'] for hit in resp['hits']['hits'])
            retrieved += batch_count
            scroll_from += batch_count

    except Exception as e:
        # 인덱스가 없으면 빈 set 반환
        if 'index_not_found_exception' in str(e):
            logger.warning(f"Index '{index_name}' not found. Returning empty set.")
            return set()
        else:
            logger.error(f"Error retrieving IDs from '{index_name}': {e}")
            raise
    return ids

# ---- Lambda 핸들러 ----
def lambda_handler(event, context):
    global es_client

    try:
        # Elasticsearch 클라이언트 지연 초기화
        if es_client is None:
            logger.info("Initializing new Elasticsearch client.")
            ES_HOST = get_parameter(ssm, '/planit/llm/es-host')
            ES_USER = get_parameter(ssm, '/planit/es-user/super')
            ES_PASSWORD = get_parameter(ssm, '/planit/es-password/super')

            es_client = Elasticsearch(
                ES_HOST,
                http_auth=(ES_USER, ES_PASSWORD),
                request_timeout=30,
                max_retries=5,
                retry_on_timeout=True
            )
            logger.info("Elasticsearch client initialized successfully.")

        # SQS URL 가져오기
        SQS_QUEUE_URL = get_parameter(ssm, '/planit/llm/sqs-queue-url')
        if not SQS_QUEUE_URL:
            raise ValueError("SQS_QUEUE_URL is not set.")

        # 인덱스 이름 설정
        KST = timezone(timedelta(hours=9))
        current_kst_time = datetime.now(tz=KST)
        current_datetime_str = current_kst_time.strftime("%Y.%m.%d_%H")

        # SOURCE_INDEX = "planit-edr-ai-training-2025.10.27_22" # 정상
        # SOURCE_INDEX = "planit-edr-ai-training-2025.10.03_15" # 악성(42)
        SOURCE_INDEX = "planit-edr-ai-training-2025.10.16_16" # 악성(16)
        # SOURCE_INDEX = "planit-edr-ai-training-2025.10.08_17" # 정상(6)
        # SOURCE_INDEX = "planit-edr-ai-training-normal" # 정상(25)
        # SOURCE_INDEX = "planit-edr-ai-training-2025.12.03" # 정상(19)
        # DEST_INDEX = f"planit-edr-ai-analyzed-{current_datetime_str}"
        DEST_INDEX = f"planit-llm-malicious"

        # 처리 완료 ID 조회
        processed_ids = get_all_ids_from_index(es_client, DEST_INDEX)
        logger.info(f"Found {len(processed_ids)} processed IDs from '{DEST_INDEX}'.")

        # 처리할 ID 조회
        source_ids = get_all_ids_from_index(es_client, SOURCE_INDEX)
        logger.info(f"Found {len(source_ids)} total IDs from '{SOURCE_INDEX}'.")

        # 새로 처리할 ID 계산
        ids_to_process = source_ids - processed_ids
        original_count = len(ids_to_process)
        logger.info(f"Found {len(ids_to_process)} new IDs to process.")

        if not ids_to_process:
            return {'statusCode': 200, 'body': 'No new documents to process.'}

        # SQS에 배치 전송
        message_list = list(ids_to_process)

        # ---- 300개 제한 로직 추가 ----
        PROCESS_LIMIT = 300
        if original_count > PROCESS_LIMIT:
            logger.warning(f"Processing limit hit: Found {original_count} new IDs, but limiting to {PROCESS_LIMIT}.")
            # 300개로 슬라이싱
            message_list = message_list[:PROCESS_LIMIT]
        # --------

        sent_count = 0
        failed_count = 0

        for i in range(0, len(message_list), 10):
            chunk = message_list[i:i+10]
            entries = []
            for unique_id in chunk:
                entries.append({
                    'Id': unique_id.replace('-', ''),
                    'MessageBody': json.dumps({'UniqueID': unique_id, 'SourceIndex': SOURCE_INDEX})
                })
            try:
                response = sqs.send_message_batch(QueueUrl=SQS_QUEUE_URL, Entries=entries)
                sent_count += len(entries)
                if 'Failed' in response and response['Failed']:
                    failed_count += len(response['Failed'])
                    for f in response['Failed']:
                        logger.error(f"Failed to send SQS message: {f}")
            except Exception as e:
                logger.error(f"SQS send_message_batch error: {e}", exc_info=True)
                failed_count += len(entries)

        logger.info(f"Message sending complete. Success: {sent_count}, Failed: {failed_count}")
        return {'statusCode': 200, 'body': f'{sent_count} messages sent, {failed_count} failed'}

    except Exception as e:
        logger.error(f"Error in Lambda handler: {e}", exc_info=True)
        raise