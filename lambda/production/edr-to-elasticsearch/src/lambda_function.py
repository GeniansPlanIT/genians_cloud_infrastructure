import helpers
import boto3
import requests
from botocore.exceptions import ClientError  # S3 예외 처리를 위해 임포트
from elasticsearch import Elasticsearch, helpers as es_helpers
from datetime import datetime, timedelta, timezone
import logging
import os
import json

# ---- boto3 클라이언트 초기화 ----
ssm = boto3.client('ssm')
s3 = boto3.client('s3')  # S3 클라이언트 추가

# ---- 로깅 설정 ----
logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())

# ---- 전역 변수 ----
ES_HOST = None
es_client = None

def lambda_handler(event, context):
    global ES_HOST, es_client
    request_id = getattr(context, "aws_request_id", "unknown")

    # Step Function 실행의 기준이 될 시간을 핸들러 시작 시점에 고정
    KST = timezone(timedelta(hours=9))
    execution_start_time = datetime.now(tz=KST)

    # 인덱스 생성용 날짜 (일 단위) -> 인덱스 폭증 방지
    DAILY_INDEX_SUFFIX = execution_start_time.strftime("%Y.%m.%d")

    # 데이터 필터링용 배치 ID (시간 단위) -> EC2가 조회할 키
    BATCH_ID = execution_start_time.strftime("%Y.%m.%d_%H")

    
    logger.info(f"Index Target: {DAILY_INDEX_SUFFIX}, Batch ID: {BATCH_ID}")

    if es_client is None:
        try:
            ES_HOST = helpers.get_parameter(ssm, '/planit/llm/es-host')
            ES_USER = helpers.get_parameter(ssm, '/planit/es-user')
            ES_PASSWORD = helpers.get_parameter(ssm, '/planit/es-password', with_decryption=True)
            es_client = Elasticsearch(
                ES_HOST,
                http_auth=(ES_USER, ES_PASSWORD),
                # scheme="https",
                request_timeout=1000000)
            logger.info("Elasticsearch 클라이언트 초기화 성공")
        except Exception as e:
            logger.error("Elasticsearch 클라이언트 초기화 실패", exc_info=True)
            return {"statusCode": 500, "body": "Elasticsearch client initialization failed."}

    # ---- S3 버킷 정보 가져오기 ----
    try:
        BUCKET_NAME = helpers.get_parameter(ssm, '/planit/s3/raw/bucket')
        PREFIX = "api/edr/" # S3 내 저장 경로 (필요에 따라 수정)
    except Exception as e:
        logger.error("S3 버킷 파라미터 조회 실패", exc_info=True)
        return {"statusCode": 500, "body": "Failed to get S3 bucket parameter."}

    # ---- 전체 결과 카운터 ----
    s3_saved_total, s3_skipped_total = 0, 0
    es_processed_total, es_failed_total = 0, 0
    
    customers = helpers.get_all_customers(ssm)
    logger.info("고객사 조회 완료", extra={"customers": customers, "request_id": request_id})

    for customer in customers:
        try:
            API_KEY = helpers.get_parameter(ssm, f'/planit/edr/{customer}/api_key', with_decryption=True)
            API_URL = helpers.get_parameter(ssm, f'/planit/edr/{customer}/api_url')
            
            API_HEADERS = {"accept": "application/json;charset=UTF-8", "apiKey": API_KEY}
            API_PARAMS = {
                "mode": "search", "range_field": "DetectTime", "period": "1d",
                "timezone": "Asia/Seoul", "sort": '[{"field":"DetectTime","dir":"desc"}]',
                "limit": 1000, "condition": 'DetectType:"XBA" AND State:"new"'
            }

            resp = requests.get(API_URL, headers=API_HEADERS, params=API_PARAMS, timeout=1000000)
            if resp.status_code != 200:
                logger.warning("EDR API 요청 실패", extra={"customer": customer, "status": resp.status_code, "body": resp.text[:300]})
                continue

            events = resp.json().get("result", {}).get("events", [])
            logger.info(f"고객사 [{customer}] API 조회 결과: {len(events)}개의 threats 발견", extra={"customer": customer, "threat_count": len(events)})

            if not events:
                logger.info(f"[{customer}] 새로운 이벤트가 없습니다.")
                continue

            bulk_actions = []
            upload_dt = datetime.now(tz=timezone(timedelta(hours=9)))
            # index_date_str = upload_dt.strftime("%Y.%m.%d_%H")
            # index_date_str = EXECUTION_INDEX_STR # 고정된 시간 사용 
            
            customer_s3_saved, customer_s3_skipped = 0, 0

            for ev in events:
                # ---- S3 저장을 위한 UniqueID 및 파일 경로 생성 ----
                detect_time = str(ev.get("DetectTime", ""))
                detect_id = str(ev.get("DetectID", ""))
                unique_id = f"{detect_time}{detect_id}"
                
                # S3 객체 키는 YYYY/MM/DD 형식으로 저장하여 파티셔닝 효율을 높입니다.
                file_key = f"{PREFIX}{customer}/{unique_id}.json"

                # ---- S3 중복 체크 ----
                try:
                    s3.head_object(Bucket=BUCKET_NAME, Key=file_key)
                    # 파일이 이미 존재하면 건너뛰기
                    customer_s3_skipped += 1
                    continue
                except ClientError as e:
                    if e.response['Error']['Code'] != "404": # 404가 아니면 실제 에러
                        logger.error("S3 head_object 실패", extra={"customer": customer, "key": file_key, "error": str(e)})
                        raise
                
                # ---- S3에 원본 저장 (파일이 없을 경우) ----
                # 저장을 위해 Customer, EventDate 필드를 먼저 추가합니다.
                ev["Customer"] = customer
                ev["EventDate"] = BATCH_ID
                
                s3.put_object(
                    Bucket=BUCKET_NAME,
                    Key=file_key,
                    Body=json.dumps(ev, ensure_ascii=False, indent=2), # 가독성을 위해 indent 추가
                    ContentType="application/json"
                )
                customer_s3_saved += 1
                
                # ---- Elasticsearch 저장을 위한 데이터 준비 ----
                # ES 문서에는 UniqueID가 필요하므로 추가합니다.
                ev["UniqueID"] = unique_id
                
                # 액션 생성 1: edr-threats 인덱스
                threats_doc = ev.copy()
                sanitized_threats_doc = helpers.sanitize_document(threats_doc)
                bulk_actions.append({
                    "_index": f"planit-edr-threats-{DAILY_INDEX_SUFFIX}",
                    "_id": unique_id,
                    "_source": sanitized_threats_doc
                })

                # 액션 생성 2: edr-ai-events 인덱스
                ai_doc = helpers.transform_for_ai_pipeline(ev)
                sanitized_ai_doc = helpers.sanitize_document(ai_doc)
                bulk_actions.append({
                    "_index": f"planit-edr-ai-events-{DAILY_INDEX_SUFFIX}",
                    "_id": unique_id,
                    "_source": sanitized_ai_doc
                })

            s3_saved_total += customer_s3_saved
            s3_skipped_total += customer_s3_skipped
            logger.info(f"고객사 [{customer}] S3 처리 결과: Saved={customer_s3_saved}, Skipped={customer_s3_skipped}")

            # ---- Elasticsearch로 데이터 Bulk 전송 ----
            if bulk_actions:
                try:
                    success, failed = es_helpers.bulk(es_client, bulk_actions, stats_only=False, raise_on_error=False)
                    es_processed_total += success
                    failed_count = len(failed) if isinstance(failed, list) else 0
                    es_failed_total += failed_count
                    
                    logger.info("고객사 ES 처리 결과", extra={"customer": customer, "success": success, "failed": failed_count})
                    if failed:
                        # 로그가 너무 길어지지 않게 실패 항목 중 5개만 기록합니다.
                        logger.warning("일부 문서 색인 실패", extra={"customer": customer, "details": failed[:5]})
                except Exception as e:
                    logger.error("Elasticsearch bulk 작업 실패", exc_info=True, extra={"customer": customer})
                    # bulk_actions에 포함된 모든 문서가 실패한 것으로 간주합니다.
                    es_failed_total += len(bulk_actions)

        except Exception as e:
            logger.error("고객사 처리 중 예외 발생", exc_info=True, extra={"customer": customer, "request_id": request_id})

    final_body = f"S3 Saved: {s3_saved_total}, S3 Skipped: {s3_skipped_total}, ES Processed: {es_processed_total}, ES Failed: {es_failed_total}"
    logger.info("Lambda 실행 완료", extra={"summary": final_body})

    return {
        "statusCode": 200,
        "body": final_body,
        # 다음 람다(Producer)가 인덱스를 찾을 때 사용
        "target_index_suffix": DAILY_INDEX_SUFFIX,
        # EC2가 데이터를 필터링할 때 사용
        "batch_id": BATCH_ID
    }