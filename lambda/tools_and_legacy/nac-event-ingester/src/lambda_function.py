import os
import json
import boto3
import requests
import logging
import time
from botocore.exceptions import ClientError
from datetime import datetime, timedelta, timezone

s3 = boto3.client('s3')
ssm = boto3.client('ssm')

# ---- logging 설정 ----
logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())

PREFIX = "api/nac/"

def get_parameter(name, with_decryption=False):
    try:
        resp = ssm.get_parameter(Name=name, WithDecryption=with_decryption)
        return resp['Parameter']['Value']
    except Exception as e:
        logger.error("SSM get_parameter 실패", extra={"param": name, "error": str(e)})
        raise

def get_all_customers():
    """
    /planit/nac 아래 모든 고객사명을 조회
    """
    paginator = ssm.get_paginator('get_parameters_by_path')
    customer_names = set()

    for page in paginator.paginate(
        Path='/planit/nac',
        Recursive=True,
        WithDecryption=True
    ):
        for param in page['Parameters']:
            # param Name: /planit/nac/<customer>/api_key 또는 api_url
            parts = param['Name'].split('/')
            if len(parts) >= 5:
                customer = parts[3]  # 0:'', 1:planit, 2:nac, 3:<customer>
                customer_names.add(customer)

    return sorted(customer_names)


def lambda_handler(event, context):
    request_id = getattr(context, "aws_request_id", "unknown")

    # logger.warning("테스트 에러")

    # S3 버킷 가져오기
    BUCKET_NAME = get_parameter('/planit/s3/raw/bucket')

    saved_total, skipped_total = 0, 0

    # 모든 고객사 조회
    customers = get_all_customers()
    logger.info("고객사 조회 완료", extra={"customers": customers, "request_id": request_id})

    for customer in customers:
        try:
            # 고객사별 API 정보
            API_KEY = get_parameter(f'/planit/nac/{customer}/api_key', with_decryption=True)
            API_URL = get_parameter(f'/planit/nac/{customer}/api_url')

            API_HEADERS = {
                "Accept": "application/json;charset=UTF-8",
                "Content-Type": "application/json"
            }

            # NAC Audit Logs API 호출 (모든 페이지)
            page = 1
            page_size = 400
            all_logs = []

            while True:
                API_PARAMS = {
                    "page": page,
                    "pageSize": page_size,
                    "logschema": "auditlog",
                    "periodType": "today",
                    "apiKey": API_KEY
                }

                try:
                    resp = requests.get(API_URL, headers=API_HEADERS, params=API_PARAMS,
                                        verify=False, timeout=30)
                except Exception as e:
                    logger.error("NAC API 요청 예외",
                                 extra={"customer": customer, "page": page, "error": str(e)})
                    break
                    
                if resp.status_code != 200:
                    logger.warning("NAC API 요청 실패",
                                extra={"customer": customer, "status": resp.status_code, "body": resp.text[:300]})
                    break

                try:
                    nac_data = resp.json()
                except json.JSONDecodeError as e:
                    logger.error("JSON 파싱 실패",
                                 extra={"customer": customer, "page": page, "error": str(e)})
                    break

                logs = nac_data.get("result", [])

                if not logs:
                    break  # 더 이상 데이터가 없으면 종료

                all_logs.extend(logs)
                page += 1  # 다음 페이지로 이동
                time.sleep(0.2)  # 서버 부하 방지 (0.2초 간격)

            upload_dt = datetime.now(tz=timezone(timedelta(hours=9)))
            # event_date = upload_dt.strftime("%Y.%m.%d")
            event_date = upload_dt.strftime("%Y.%m.%d_%H")

            saved, skipped = 0, 0

            for log in all_logs:
                log["EventDate"] = event_date
                log["Customer"] = customer

                # _id → id
                log_id = log.pop("_id", None)
                if not log_id:
                    continue
                log["id"] = log_id

                # _type → type
                log_type = log.pop("_type", None)
                if not log_type:
                    continue
                log["type"] = log_type

                # _index → nac_index
                log_index = log.pop("_index", None)
                if not log_index:
                    continue
                log["nac_index"] = log_index

                # # UniqueID 생성
                # unique_id = f"{event_date}_{log_id}"
                # log["UniqueID"] = unique_id

                file_key = f"{PREFIX}{customer}/{log_id}.json"

                # 중복 체크
                try:
                    s3.head_object(Bucket=BUCKET_NAME, Key=file_key)
                    skipped += 1
                    continue
                except ClientError as e:
                    if e.response['Error']['Code'] != "404":
                        logger.error("S3 head_object 실패",
                                     extra={"customer": customer, "key": file_key, "error": str(e)})
                        raise

                # S3 저장
                s3.put_object(
                    Bucket=BUCKET_NAME,
                    Key=file_key,
                    Body=json.dumps(log, ensure_ascii=False),
                    ContentType="application/json"
                )
                saved += 1

            logger.info("고객사 처리 결과", extra={"customer": customer, "saved": saved, "skipped": skipped})
            print(f"[{customer}] Saved: {saved}, Skipped: {skipped}")

            saved_total += saved
            skipped_total += skipped

        except Exception as e:
            logger.error("고객사 처리 중 예외",
                         extra={"customer": customer, "error": str(e), "request_id": request_id})
            continue  # 다음 고객사 진행

    return {
        "statusCode": 200,
        "body": f"Total Saved: {saved_total}, Total Skipped: {skipped_total}"
    }
