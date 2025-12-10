import os
import json
import boto3
import requests
import logging
import time
from botocore.exceptions import ClientError
from datetime import datetime, timedelta, timezone
# Elasticsearch 라이브러리 임포트
from elasticsearch import Elasticsearch, helpers as es_helpers

s3 = boto3.client('s3')
ssm = boto3.client('ssm')

# ---- logging 설정 ----
logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())

PREFIX = "api/nac/"

# ---- 전역 변수 ----
es_client = None

def get_parameter(name, with_decryption=False):
    try:
        resp = ssm.get_parameter(Name=name, WithDecryption=with_decryption)
        return resp['Parameter']['Value']
    except Exception as e:
        logger.error("SSM get_parameter 실패", extra={"param": name, "error": str(e)})
        raise

def get_all_customers():
    paginator = ssm.get_paginator('get_parameters_by_path')
    customer_names = set()
    for page in paginator.paginate(Path='/planit/nac', Recursive=True, WithDecryption=True):
        for param in page['Parameters']:
            parts = param['Name'].split('/')
            if len(parts) >= 5:
                customer = parts[3]
                customer_names.add(customer)
    return sorted(customer_names)

def lambda_handler(event, context):
    global es_client
    request_id = getattr(context, "aws_request_id", "unknown")

    # ---- Elasticsearch 클라이언트 초기화 ----
    if es_client is None:
        try:
            ES_HOST = get_parameter('/planit/llm/es-host')
            ES_USER = get_parameter('/planit/es-user')
            ES_PASSWORD = get_parameter('/planit/es-password', with_decryption=True)
            
            es_client = Elasticsearch(
                ES_HOST,
                http_auth=(ES_USER, ES_PASSWORD),
                request_timeout=10000000
            )
            logger.info("Elasticsearch 클라이언트 초기화 성공")
        except Exception as e:
            logger.error("Elasticsearch 클라이언트 초기화 실패", exc_info=True)
            return {"statusCode": 500, "body": "Elasticsearch client initialization failed."}

    # S3 버킷 가져오기
    BUCKET_NAME = get_parameter('/planit/s3/raw/bucket')

    s3_saved_total, s3_skipped_total = 0, 0
    es_processed_total, es_failed_total = 0, 0

    customers = get_all_customers()
    logger.info("고객사 조회 완료", extra={"customers": customers, "request_id": request_id})

    for customer in customers:
        try:
            API_KEY = get_parameter(f'/planit/nac/{customer}/api_key', with_decryption=True)
            API_URL = get_parameter(f'/planit/nac/{customer}/api_url')
            API_HEADERS = {"Accept": "application/json;charset=UTF-8", "Content-Type": "application/json"}

            page, page_size, all_logs = 1, 400, []
            while True:
                API_PARAMS = { "page": page, "pageSize": page_size, "logschema": "auditlog", "periodType": "today", "apiKey": API_KEY }
                try:
                    resp = requests.get(API_URL, headers=API_HEADERS, params=API_PARAMS, verify=False, timeout=10000000)
                except Exception as e:
                    # logger.error("NAC API 요청 예외", extra={"customer": customer, "page": page, "error": str(e)})
                    logger.error("NAC API 요청 예외", exc_info=True, extra={"customer": customer, "page": page})
                    break
                
                if resp.status_code != 200:
                    logger.warning("NAC API 요청 실패", extra={"customer": customer, "status": resp.status_code, "body": resp.text[:300]})
                    break
                
                try:
                    logs = resp.json().get("result", [])
                    if not logs: break
                    all_logs.extend(logs)
                    page += 1
                    time.sleep(0.2)
                except json.JSONDecodeError:
                    logger.error("JSON 파싱 실패", extra={"customer": customer, "page": page, "body": resp.text[:300]})
                    break

            if not all_logs:
                logger.info(f"[{customer}] 새로운 로그가 없습니다.")
                continue

            upload_dt = datetime.now(tz=timezone(timedelta(hours=9)))
            index_date_str = upload_dt.strftime("%Y.%m")
            
            s3_saved, s3_skipped = 0, 0
            bulk_actions = [] # Elasticsearch로 보낼 데이터를 담을 리스트

            for log in all_logs:
                log_id = log.pop("_id", None)
                if not log_id: continue
                
                log["id"] = log_id
                log["type"] = log.pop("_type", None)
                log["nac_index"] = log.pop("_index", None)
                log["EventDate"] = index_date_str
                log["Customer"] = customer

                # S3 중복 체크 및 저장
                file_key = f"{PREFIX}{customer}/{log_id}.json"
                try:
                    s3.head_object(Bucket=BUCKET_NAME, Key=file_key)
                    s3_skipped += 1
                    continue # S3에 이미 있으면 ES에도 있다고 가정하고 건너뜀
                except ClientError as e:
                    if e.response['Error']['Code'] != "404": raise
                
                s3.put_object(
                    Bucket=BUCKET_NAME, Key=file_key,
                    Body=json.dumps(log, ensure_ascii=False),
                    ContentType="application/json"
                )
                s3_saved += 1
                
                # Elasticsearch bulk action 생성
                bulk_actions.append({
                    "_index": f"planit-nac-auditlogs-{index_date_str}",
                    "_id": log_id,
                    "_source": log
                })

            s3_saved_total += s3_saved
            s3_skipped_total += s3_skipped
            logger.info(f"[{customer}] S3 처리 결과: Saved={s3_saved}, Skipped={s3_skipped}")

            # Elasticsearch로 데이터 Bulk 전송
            if bulk_actions:
                try:
                    # stats_only를 False로 변경하거나 제거하여 실패 시 상세 정보를 받습니다.
                    success, failed_items = es_helpers.bulk(es_client, bulk_actions, raise_on_error=False)

                    es_processed_total += success
                    es_failed_total += len(failed_items)
                    
                    logger.info(f"[{customer}] ES 처리 결과: Success={success}, Failed={len(failed_items)}")

                    # 실패한 항목이 있다면, 그 상세 내용을 로그로 남깁니다.
                    if failed_items:
                        # logger.error("ES 색인 실패 상세 정보", extra={"details": failed_items[:5]}) # 최대 5개만 로깅
                        details_str = json.dumps(failed_items[:5], indent=2, ensure_ascii=False, default=str)
                        logger.error(f"ES 색인 실패 상세 정보 (상위 5개): {details_str}")
                except Exception as e:
                    logger.error("Elasticsearch bulk 작업 실패", exc_info=True, extra={"customer": customer})
                    es_failed_total += len(bulk_actions)

        except Exception as e:
            logger.error("고객사 처리 중 예외", extra={"customer": customer, "error": str(e), "request_id": request_id})
            continue

    final_body = (f"Total S3 Saved: {s3_saved_total}, Total S3 Skipped: {s3_skipped_total}, "
                  f"Total ES Processed: {es_processed_total}, Total ES Failed: {es_failed_total}")
    logger.info("Lambda 실행 완료", extra={"summary": final_body})
    
    return {
        "statusCode": 200,
        "body": final_body
    }