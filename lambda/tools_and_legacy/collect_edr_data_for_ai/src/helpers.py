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
    """/planit/edr 아래 모든 고객사명을 조회하는 함수"""
    paginator = ssm_client.get_paginator('get_parameters_by_path')
    customer_names = set()
    for page in paginator.paginate(Path='/planit/edr', Recursive=True, WithDecryption=True):
        for param in page['Parameters']:
            parts = param['Name'].split('/')
            if len(parts) >= 5:
                customer = parts[3]
                customer_names.add(customer)
    return sorted(customer_names)

def transform_for_ai_pipeline(event_doc):
    """필터 로직을 수행하는 함수"""
    
    # 1. 원본 복사하여 작업
    doc = event_doc.copy()

    # 2. 날짜 필드 변환 
    if '@timestamp' not in doc:
        if 'DetectTime' in doc and isinstance(doc['DetectTime'], (int, float)):
            doc['@timestamp'] = datetime.fromtimestamp(doc['DetectTime'] / 1000, tz=timezone.utc).isoformat()

    # 3. 필드 타입 변환 (Logstash의 mutate.convert 역할)
    for field in ['Level', 'Score']:
        if field in doc and doc[field] is not None:
            try:
                doc[field] = int(doc[field])
            except (ValueError, TypeError):
                logger.warning(f"필드 {field}를 정수로 변환할 수 없습니다.", extra={"value": doc[field]})

    # 4. 중첩된 JSON 파싱 (Logstash의 json 필터 역할)
    # .strip()으로 공백 문자열까지 확인하여 비어있지 않을 때만 파싱
    if 'ResponseInfo' in doc and isinstance(doc['ResponseInfo'], str) and doc['ResponseInfo'].strip():
        try:
            doc['ResponseInfo'] = json.loads(doc['ResponseInfo'])
        except json.JSONDecodeError:
            # 어떤 데이터가 실패했는지 로그를 남기면 디버깅에 더 좋습니다.
            logger.warning("ResponseInfo 필드 파싱 실패", extra={"value": doc['ResponseInfo']})

    # # Memo 필드 파싱 및 threat_label 필드 생성
    # if 'Memo' in doc and isinstance(doc['Memo'], str) and doc['Memo'].strip():
    #     try:
    #         # Memo 필드의 JSON 문자열을 파싱
    #         # parsed_memo = json.loads(doc['Memo'])
    #         cleaned_str = doc['Memo'].replace('\u00a0', ' ')
    #         parsed_memo = json.loads(cleaned_str)
    #         # 파싱된 객체 안에 'threat_label' 키가 있는지 확인
    #         if 'threat_label' in parsed_memo:
    #             # 최상위 필드로 'threat_label' 객체를 추가
    #             doc['threat_label'] = parsed_memo['threat_label']
    #     except json.JSONDecodeError:
    #         # logger.warning("Memo 필드 파싱 실패", extra={"value": doc['Memo']})
    #         # logger.warning(
    #         #     "Memo 필드 파싱 실패",
    #         #     extra={"unique_id": doc.get("UniqueID"), "value": doc['Memo']}
    #         # )
    #         logger.warning(
    #             "Memo 필드 파싱 실패",
    #             extra={
    #                 "unique_id": doc.get("UniqueID"), 
    #                 "value_repr": repr(doc['Memo'])  # 이 부분을 추가/변경하세요
    #             }
    #         )
    if 'Memo' in doc and isinstance(doc['Memo'], str) and doc['Memo'].strip():
        try:
            cleaned_str = doc['Memo'].replace('\u00a0', ' ')
            # JSON이 }로 끝나지 않으면 닫기 시도
            if cleaned_str.count('{') > cleaned_str.count('}'):
                cleaned_str += '}' * (cleaned_str.count('{') - cleaned_str.count('}'))
            parsed_memo = json.loads(cleaned_str)
            if 'threat_label' in parsed_memo:
                doc['threat_label'] = parsed_memo['threat_label']
        except json.JSONDecodeError:
            logger.warning(
                "Memo 필드 파싱 실패",
                extra={"unique_id": doc.get("UniqueID"), "value": doc['Memo']}
            )
            

    if 'event' in doc and isinstance(doc.get('event'), dict) and isinstance(doc['event'].get('original'), str):
        try:
            pass
        except json.JSONDecodeError:
            logger.warning("[event][original] 필드 파싱 실패")

    # 5. 불필요한 필드 제거 (Logstash의 mutate.remove_field 역할)
    doc.pop('event', None)
    doc.pop('log', None)

    # 6. 필드 화이트리스팅 (Logstash의 prune 필터 역할)
    whitelist = {
        "UniqueID", "Customer", "ThreatID", "DetectTime", "EventTime", "@timestamp", "EventDate",
        "RuleID", "RuleName", "DetectSubType", "EventType", "EventSubType", "Classification",
        "IsKnown", "Level", "Score", "HostName", "Platform", "IP", "NATIP", "AuthName",
        "AuthDeptName", "ProcName", "ProcPath", "CmdLine", "FileName", "FileType",
        "MD5", "SHA256", "Tactic", "TacticID", "Technique", "TechniqueID",
        "SuspiciousInfo", "SuspiciousInfo2", "SuspiciousInfo3", "TrunkID", "ChildTrunkID", "ResponseInfo", "threat_label"
    }
    
    pruned_doc = {key: value for key, value in doc.items() if key in whitelist}
    
    return pruned_doc

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