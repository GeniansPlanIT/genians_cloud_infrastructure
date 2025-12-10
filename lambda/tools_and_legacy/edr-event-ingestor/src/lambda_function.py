import os
import json
import boto3
import requests
import logging 
from botocore.exceptions import ClientError
from datetime import datetime, timedelta, timezone

s3 = boto3.client('s3')
ssm = boto3.client('ssm')

# ---- logging 설정 ----
logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())  # 필요시 콘솔/파라미터로 제어

PREFIX = "api/edr/"

def get_parameter(name, with_decryption=False):
    try:
        resp = ssm.get_parameter(Name=name, WithDecryption=with_decryption)
        return resp['Parameter']['Value']
    except Exception as e:
        logger.error("SSM get_parameter 실패", extra={"param": name, "error": str(e)})
        raise

def get_all_customers():
    """
    /planit/edr 아래 모든 고객사명을 조회
    """
    paginator = ssm.get_paginator('get_parameters_by_path')
    customer_names = set()

    for page in paginator.paginate(
        Path='/planit/edr',
        Recursive=True,
        WithDecryption=True
    ):
        for param in page['Parameters']:
            # param Name: /planit/edr/<customer>/api_key 또는 api url
            parts = param['Name'].split('/')
            if len(parts) >= 5:
                customer = parts[3] # 0: '', 1: planit, 2: edr, 3: <customer>
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
            API_KEY = get_parameter(f'/planit/edr/{customer}/api_key', with_decryption=True)
            API_URL = get_parameter(f'/planit/edr/{customer}/api_url')

            API_HEADERS = {
                "accept": "application/json;charset=UTF-8",
                "apiKey": API_KEY
            }
            API_PARAMS = {
                "mode": "search",
                "range_field": "DetectTime",
                "period": "1d",
                "timezone": "Asia/Seoul",
                "sort": '[{"field":"DetectTime","dir":"desc"}]',
                "limit": 1000,
                "condition": "State:new"
            }

            # EDR API 호출
            resp = requests.get(API_URL, headers=API_HEADERS, params=API_PARAMS, timeout=30)
            if resp.status_code != 200:
                logger.warning("EDR API 요청 실패",
                               extra={"customer": customer, "status": resp.status_code, "body": resp.text[:300]})
                continue

            edr_data = resp.json()
            events = edr_data.get("result", {}).get("events", [])

            upload_dt = datetime.now(tz=timezone(timedelta(hours=9)))
            event_date = upload_dt.strftime("%Y.%m.%d_%H")

            saved, skipped = 0, 0

            for ev in events:
                ev["EventDate"] = event_date
                detect_time = str(ev.get("DetectTime", ""))
                detect_id = str(ev.get("DetectID", ""))
                unique_id = f"{detect_time}{detect_id}"

                # UniqueID 필드 추가
                ev["UniqueID"] = unique_id
                # 고객사명 필드 추가
                ev["Customer"] = customer

                file_key = f"{PREFIX}{customer}/{unique_id}.json"

                # 중복 체크
                try:
                    s3.head_object(Bucket=BUCKET_NAME, Key=file_key)
                    skipped += 1
                    continue
                except ClientError as e:
                    if e.response['Error']['Code'] != "404":
                        logger.error("S3 head_object 실패", extra={"customer": customer, "key": file_key, "error": str(e)})
                        raise

                # 저장
                s3.put_object(
                    Bucket=BUCKET_NAME,
                    Key=file_key,
                    Body=json.dumps(ev, ensure_ascii=False),
                    ContentType="application/json"
                )
                saved += 1

            logger.info("고객사 처리 결과", extra={"customer": customer, "saved": saved, "skipped": skipped})
            print(f"[{customer}] Saved: {saved}, Skipped: {skipped}")
            saved_total += saved
            skipped_total += skipped

        except Exception as e:
            # 여기서 error 로그를 남기면 CloudWatch 구독 필터가 잡아서 Slack으로 전달합니다.
            logger.error("고객사 처리 중 예외",
                         extra={"customer": customer, "error": str(e), "request_id": request_id})
            # 필요시 계속 다음 고객사 진행(continue)

    return {
        "statusCode": 200,
        "body": f"Total Saved: {saved_total}, Total Skipped: {skipped_total}"
    }