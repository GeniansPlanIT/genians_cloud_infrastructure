import logging
import json
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# ssm_client를 첫 번째 인자로 받도록 수정
def get_parameter(ssm_client, name, with_decryption=False):
    """SSM 파라미터 스토어에서 값을 가져오는 함수"""
    try:
        resp = ssm_client.get_parameter(Name=name, WithDecryption=with_decryption)
        return resp['Parameter']['Value']
    except Exception as e:
        logger.error("SSM get_parameter 실패", extra={"param": name, "error": str(e)})
        raise

def get_all_customers(ssm_client):
    """/planit/nac 아래 모든 고객사명을 조회하는 함수"""
    paginator = ssm_client.get_paginator('get_parameters_by_path')
    customer_names = set()
    for page in paginator.paginate(Path='/planit/nac', Recursive=True, WithDecryption=True):
        for param in page['Parameters']:
            parts = param['Name'].split('/')
            if len(parts) >= 5:
                customer = parts[3]
                customer_names.add(customer)
    return sorted(customer_names)

def sanitize_document(doc):
    """딕셔너리 내의 빈 문자열("") 값을 None으로 변환하는 함수"""
    sanitized_doc = {}
    for key, value in doc.items():
        # 값이 비어있는 문자열이라면 None으로 변경
        if value == "":
            sanitized_doc[key] = None
        else:
            sanitized_doc[key] = value
    return sanitized_doc