import helpers
import boto3
import requests
from botocore.exceptions import ClientError  # S3 예외 처리를 위해 임포트
from elasticsearch import Elasticsearch, helpers as es_helpers
from datetime import datetime, timedelta, timezone
import logging
import os
import json
import time  # API 재시도를 위해 추가
import sys   # 로거 설정을 위해 추가

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

    if es_client is None:
        try:
            ES_HOST = helpers.get_parameter(ssm, '/planit/llm/es-host')
            ES_USER = helpers.get_parameter(ssm, '/planit/es-user/super')
            ES_PASSWORD = helpers.get_parameter(ssm, '/planit/es-password/super', with_decryption=True)
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
        BUCKET_NAME = helpers.get_parameter(ssm, '/planit/s3/ai/bucket')
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
                "mode": "search", "range_field": "DetectTime", "to_pattern": "2025-10-16T00:00:00", "from_pattern": "2025-10-10T00:00:00",
                "timezone": "Asia/Seoul", "sort": '[{"field":"DetectTime","dir":"desc"}]',
                "limit": 1000, "condition": "State:resolved"
            }
            
            # API_PARAMS = {
            #     "mode": "search", "range_field": "DetectTime", "to_pattern": "2025-10-10T00:00:00", "from_pattern": "2025-10-16T00:00:00",
            #     "timezone": "Asia/Seoul", "sort": '[{"field":"DetectTime","dir":"desc"}]',
            #     "limit": 1000, "condition": 'AssigneeName:"강서현" AND State:"resolved" AND AlertDecision:"false"'
            # }

            # 재시도를 위한 설정
            max_retries = 3
            retry_delay = 5 # 초

            for attempt in range(max_retries):
                try:
                    resp = requests.get(API_URL, headers=API_HEADERS, params=API_PARAMS, timeout=1000000)
                    
                    # 요청이 성공하면 루프를 빠져나감
                    if resp.status_code == 200:
                        break 
                    else:
                        logger.warning(f"API 요청 실패 (시도 {attempt + 1}/{max_retries}): {resp.status_code}")

                except ConnectTimeout:
                    logger.warning(f"API 연결 시간 초과 (시도 {attempt + 1}/{max_retries})")
                
                # 마지막 시도가 아니면 잠시 대기 후 재시도
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    logger.error("최대 재시도 횟수 초과. API 호출 최종 실패.")
                    # 실패 시 처리를 위해 resp 변수가 없는 경우를 대비
                    resp = None 
                    break # 루프 종료

            # API 호출이 최종적으로 실패했는지 확인
            if resp is None or resp.status_code != 200:
                logger.error("API로부터 데이터를 가져오지 못했습니다.")
                continue # 다음 customer 루프로 넘어감

            events = resp.json().get("result", {}).get("events", [])
            logger.info(f"고객사 [{customer}] API 조회 결과: {len(events)}개의 threats 발견", extra={"customer": customer, "threat_count": len(events)})

            if not events:
                logger.info(f"[{customer}] 새로운 이벤트가 없습니다.")
                continue

            bulk_actions = []
            upload_dt = datetime.now(tz=timezone(timedelta(hours=9)))
            index_date_str = upload_dt.strftime("%Y.%m.%d_%H")
            
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
                ev["EventDate"] = index_date_str
                
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

                # 액션 생성: planit-edr-ai-training 인덱스
                ai_doc = helpers.transform_for_ai_pipeline(ev)
                sanitized_ai_doc = helpers.sanitize_document(ai_doc)
                bulk_actions.append({
                    "_index": f"planit-edr-ai-training-{index_date_str}",
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
                        # logger.warning("일부 문서 색인 실패", extra={"customer": customer, "details": failed[:5]})
                        # 상세 실패 내역을 읽기 쉬운 JSON 문자열로 변환
                        # ensure_ascii=False는 한글 깨짐 방지, indent=2는 자동 줄 바꿈
                        failed_details_str = json.dumps(failed[:5], ensure_ascii=False, indent=2)
                        
                        # 변환된 문자열을 f-string을 이용해 로그 메시지에 직접 포함하여 출력
                        logger.warning(f"일부 문서 색인 실패. Details: {failed_details_str}")
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
        "body": final_body
    }