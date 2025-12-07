import os
import json
import time
import logging
import math
from datetime import datetime, timedelta, timezone

import boto3
from elasticsearch import Elasticsearch, helpers, ConnectionError, TransportError
from elastic_transport import ConnectionTimeout
from openai import OpenAI

# ---- 로깅 설정 ----
logger = logging.getLogger()
logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())

# ---- 글로벌 클라이언트 ----
ssm = boto3.client('ssm')
secrets_client = boto3.client('secretsmanager')
es_client = None
openai_client = None

# ---- 헬퍼 함수 ----
def get_parameter(ssm_client, name, with_decryption=True):
    try:
        response = ssm_client.get_parameter(Name=name, WithDecryption=with_decryption)
        return response['Parameter']['Value']
    except Exception as e:
        logger.error(f"SSM에서 '{name}' 파라미터를 가져오는 데 실패했습니다.", exc_info=True)
        raise e

def initialize_clients():
    """ES 및 OpenAI 클라이언트 초기화 (재시도 및 안정성 강화)"""
    global es_client, openai_client
    if es_client is None or openai_client is None:
        logger.info("Initializing ES and OpenAI clients...")

        ES_HOST = get_parameter(ssm, '/planit/llm/es-host')
        ES_USER = get_parameter(ssm, '/planit/es-user/super')
        ES_PASSWORD = get_parameter(ssm, '/planit/es-password/super')

        # Elasticsearch 안정화 옵션
        es_client = Elasticsearch(
            hosts=[ES_HOST],
            basic_auth=(ES_USER, ES_PASSWORD),
            request_timeout=60,
            max_retries=5,
            retry_on_timeout=True
        )

        SECRET_NAME = get_parameter(ssm, '/planit/llm/secret-name')
        secret = json.loads(secrets_client.get_secret_value(SecretId=SECRET_NAME)['SecretString'])
        openai_client = OpenAI(api_key=secret['OPENAI_API_KEY'])
        logger.info("Clients initialized successfully.")

def fetch_document_with_retry(index, doc_id, retries=3, delay=2):
    """ES 문서 가져오기 재시도!"""
    for attempt in range(retries):
        try:
            return es_client.get(index=index, id=doc_id)
        except (ConnectionTimeout, ConnectionError, TransportError) as e:
            logger.warning(f"ES connection failed (attempt {attempt+1}/{retries}): {e}")
            time.sleep(delay)
        except Exception as e:
            logger.error(f"Unexpected error fetching ES document: {e}", exc_info=True)
            break
    return None

# related_events의 값이 없음 명확히 표시 
def clean_data_for_llm(data, placeholders=None):
    """
    데이터를 LLM에 전달하기 전에 재귀적으로 정리
    -1, "-", "" 등 "없음"을 의미하는 값들을 None (JSON 'null')으로 통일
    """
    if placeholders is None:
        # "없음"을 의미하는 값들. 필요에 따라 "N/A" 등을 추가할 수 있음
        placeholders = [-1, "-", ""]

    if isinstance(data, dict):
        # 딕셔너리인 경우, 각 value에 대해 재귀 호출
        return {k: clean_data_for_llm(v, placeholders) for k, v in data.items()}
    
    if isinstance(data, list):
        # 리스트인 경우, 각 항목에 대해 재귀 호출
        return [clean_data_for_llm(item, placeholders) for item in data]

    # 값(str, int, float 등)인 경우
    if data in placeholders:
        return None  # None으로 통일
    
    return data


# 주변 이벤트 검색을 위한 함수
def fetch_related_events(es_client, hostname, timestamp_str, main_event_id, index_pattern="edr-syslog-fixed*", window_seconds=60, size=20):
    """
    주어진 호스트와 시간대를 기준으로 주변 로그 ES에 검색
    """
    if not hostname or not timestamp_str:
        logger.warning("Cannot fetch related events: HostName or @timestamp is missing from main event.")
        return []

    try:
        # 'Z' (UTC)를 파이썬이 인식 가능한 '+00:00'으로 변경
        event_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
    except (ValueError, TypeError):
        logger.error(f"Invalid @timestamp format, cannot parse: {timestamp_str}")
        return []

    start_time = (event_time - timedelta(seconds=window_seconds)).isoformat()
    end_time = (event_time + timedelta(seconds=window_seconds)).isoformat()

    # 주변 로그 검색 쿼리
    query_body = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"edr.HostName": hostname}}, # .keyword 대신 match 사용 (필드 타입 유연성)
                    {"range": {"@timestamp": {"gte": start_time, "lte": end_time}}}
                ],
                # 노이즈 제거: 문맥 파악에 중요한 이벤트 타입만 필터링
                "filter": [
                    {
                        "bool": {
                            "should": [
                                {"match": {"edr.EventType": "process"}},  # 프로세스 실행/종료
                                {"match": {"edr.EventType": "network"}},  # 통신 시도
                                {"match": {"edr.EventType": "file"}},     # 파일 생성/삭제 (랜섬웨어 등)
                                {"match": {"edr.EventType": "registry"}}  # 자동 실행 등록
                            ],
                            "minimum_should_match": 1
                        }
                    }
                ]
            }
        },
        "sort": [{"@timestamp": {"order": "asc"}}], # 시간순 정렬
        # 프롬프트 크기를 줄이기 위해 필요한 최소한의 필드만 요청
        "_source": [
            # 1. 필수 식별 정보
            "@timestamp",
            "edr.HostName",
            "edr.ProcUserID",
            
            # 2. 이벤트 분류
            "edr.EventType",
            "edr.EventSubType",
            
            # 3. 프로세스 행위
            "edr.ProcName",
            "edr.ProcPath",
            "edr.CmdLine",
            "edr.ParentProcName",
            "edr.ParentProcPath",
            "edr.ParentProcCmdLine",

            # 4. 네트워크 행위
            "edr.Direction",
            "edr.RemoteIP",
            "edr.RemotePort",
            "edr.DNSName",
            
            # 5. 파일/레지스트리 행위
            "edr.FileName",
            "edr.FilePath",
            "edr.RegKeyPath",
            "edr.RegValueName"
        ],
        "size": size
    }

    # 디버깅 로그 추가
    # Lambda의 CloudWatch 로그에서 이 값들을 확인하기
    logger.info(f"[DEBUG] Related Events Query - HostName: {hostname}")
    logger.info(f"[DEBUG] Related Events Query - Time Range: {start_time} to {end_time}")

    # Kibana에서 사용할 쿼리 본문 전체를 JSON 문자열로 출력
    logger.info(f"[DEBUG] Related Events Query - ES Body: {json.dumps(query_body, ensure_ascii=False)}")

    try:
        response = es_client.search(
            index=index_pattern, # EDR, Syslog 등 관련 로그가 있는 모든 인덱스 패턴
            body=query_body
        )
        # _source 데이터만 리스트로 반환
        return [hit['_source'] for hit in response['hits']['hits']]
    except (ConnectionTimeout, ConnectionError, TransportError) as e:
        logger.warning(f"ES search for related events failed: {e}")
        return [] # 주변 로그 검색에 실패해도 메인 이벤트 처리는 계속되도록 빈 리스트 반환
    except Exception as e:
        logger.error(f"Unexpected error fetching related events: {e}", exc_info=True)
        return []


def call_openai_api(current_openai_client, doc_source, related_events=None):
    """OpenAI API 호출(self-evaluation)"""

    if related_events is None:
        related_events = []

    detect_desc = doc_source.get("DetectDesc", {})

    # 핵심 필드 선별
    main_event_context = {
        "NATIP": doc_source.get("NATIP"),
        "HostName": doc_source.get("HostName"),
        "IP": doc_source.get("IP"),
        "AuthName": doc_source.get("AuthName"),
        "Platform": doc_source.get("Platform"),

        "EventTime": doc_source.get("EventTime"), 
        "EventType": doc_source.get("EventType"),
        "EventSubType": doc_source.get("EventSubType"),
        "ProcName": doc_source.get("ProcName"), 
        "FileName": doc_source.get("FileName"),
        "CmdLine": doc_source.get("CmdLine"),   
        "ProcPath": doc_source.get("ProcPath"),
        "DetectTime": doc_source.get("DetectTime"),

        "PathInfo": doc_source.get("PathInfo"),
        "PathInfo2": doc_source.get("PathInfo2"),

        # Persistence 공격 판단을 위한 레지스트리 3대장
        "RegKeyPath": detect_desc.get("RegKeyPath") or doc_source.get("RegKeyPath"),
        "RegValueName": detect_desc.get("RegValueName") or doc_source.get("RegValueName"),
        "RegValue": detect_desc.get("RegValue") or doc_source.get("RegData"),

        # Why (Detection)
        "RuleName": doc_source.get("RuleName"),
        "RuleID": doc_source.get("RuleID"),
        "DetectSubType": doc_source.get("DetectSubType"),
        "ThreatID": doc_source.get("ThreatID"),
        # "IsKnown": doc_source.get("IsKnown"),

        # Why (MITRE)
        "Tactic": doc_source.get("Tactic"),
        "TacticID": doc_source.get("TacticID"),
        "Technique": doc_source.get("Technique"),
        "TechniqueID": doc_source.get("TechniqueID"),

        # What (Detailed Context - Nested Objects)
        "ResponseInfo": doc_source.get("ResponseInfo"),
        "SuspiciousInfo": doc_source.get("SuspiciousInfo"),
        "SuspiciousInfo2": doc_source.get("SuspiciousInfo2")

    }

    # # 빈 값(None, "")  제거
    # main_event_context = {k: v for k, v in main_event_context.items() if v not in [None, ""]}

    # -1, "-", "" 등을 모두 None (null)으로 통일
    cleaned_main_event = clean_data_for_llm(main_event_context)
    
    # related_events 리스트 전체에도 재귀적으로 적용
    cleaned_related_events = clean_data_for_llm(related_events)

    # 프롬프트에 사용할 전체 데이터 구조
    context_data = {
        "main_event": cleaned_main_event,
        "related_events_60sec_window": cleaned_related_events # fetch_related_events에서 이미 _source만 추출됨
    }

    # 프롬프트 엔지니어링
    prompt = f"""
    You are a Tier 3 Threat Hunter specialized in **Contextual Anomaly Detection**.
    
    **Your Core Objective:**
    Distinguish between **True Threats (APT, Malware)** and **Benign Administrative Activities (Software deployment, Debugging, Remote management)**.
    - Missing a real threat is bad (False Negative).
    - But flooding the SOC with false alarms on admin activity is ALSO bad (False Positive).
    - **Balance is key.**

    **🚨 CRITICAL OVERRIDE RULE - SIMULATIONS & DRILLS:**
    - If you see indicators of **Atomic Red Team**, **Red Canary**, **Breach and Attack Simulation (BAS)**, or command lines explicitly mentioning "Test", "Simulation", "victim" (e.g., `curl ... atomic-red-team ...`):
        1. **Result:** MUST be **1 (Malicious)**. (We need to prove detection).
        2. **Confidence:** MUST be **Low (60-65)**.
        3. **Reason:** State clearly "Detected Security Simulation / Drill Activity".
    - **Why?** This allows the SOC to see the alert (Result 1) but prioritize it lower than real APT attacks (Confidence 90+).

    **Analysis Process (Chain of Thought):**
    1.  **Analyze Context:** Look at the `main_event` within the `related_events_60sec_window`.
    2.  **Devil's Advocate (Crucial):** Before deciding, force yourself to find reasons why your initial gut feeling might be WRONG.
        - If it looks malicious: Ask "Could this be a clumsy admin or a weird scheduled task?"
        - If it looks normal: Ask "Could this be a stealthy attacker blending in (Living off the Land)?"
    3.  **Verdict:** Only decide after weighing both sides.
    4.  **Scoring:** Assign confidence strictly based on the rubric below.

    **Critical Instructions for Classification:**
    1.  **Analyze Context:** Analyze the `main_event` in the *context* of the `related_events_60sec_window`.
    2.  **'Normal' vs 'Malicious':** A single admin tool (like powershell.exe) in `main_event` might look suspicious alone. But if the `related_events` show normal preceding activity (like logging in, opening admin tools), it is likely 'normal'.
    3.  **'Malicious' Indicators:** Look for suspicious *sequences*, like Office apps (winword.exe, excel.exe) launching powershell.exe, or downloads followed by execution from temp folders.

    ---
    **Analyze the following data package:**
    (The `related_events_60sec_window` shows other logs from the same host +-60 seconds around the `main_event`)
    {json.dumps(context_data, ensure_ascii=False, indent=2)}

    ---
    Classify the `main_event` (NOT the related events) as 'malicious' or 'normal' and explain your reasoning *based on the full context*.

    ---
    Classify the `main_event` using the following codes:
    - **0**: Normal (Safe, Benign)
    - **1**: Malicious (Threat, Suspicious)

    ---
    **Your Task:**
    1. Determine if the event is Normal (0) or Malicious (1).
    2. **Self-Evaluate your confidence score (0-100)** based on the evidence strength.

    **Confidence Scoring Rubric (Strict enforcement):**
    - **95-100:** Absolute Certainty. Matches a known APT signature, hash, or exact MITRE attack pattern with NO benign explanation.
    - **80-94:** High Confidence. Strong anomaly (e.g., encoded PowerShell, credential dumping attempt) but theoretically possible by an admin.
    - **60-79:** Suspicious. Weird behavior, but lacks context or could be a false positive.
    - **0-59:** Low Confidence. Insufficient data, generic logs, or highly likely to be noise.

    Respond ONLY in this JSON format:
    {{
    "result": 0, // Use integer 0 or 1 only
    "reason": "The core reason for your judgment, referencing the main_event and any relevant context from related_events",
    "confidence": 95 // Your self-evaluated score (Integer 0-100)
    }}
    """

    # ---- 프롬프트 출력 추가! ----
    print("==== Generated Prompt(with Context) ====")
    print(prompt)
    print("==========================")
    
    try:
        response = current_openai_client.chat.completions.create(
            model="gpt-5",
            messages=[
                {"role": "system", "content": "You are a top-tier cybersecurity analyst."},
                {"role": "user", "content": prompt}
            ],
            # Structured Outputs (Schema)
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "threat_classification",
                    "strict": True, # 스키마 엄격 준수 모드
                    "schema": {
                        "type": "object",
                        "properties": {
                            # 1. 분석 내용 요약
                            "analysis_summary": {
                                "type": "string",
                                "description": "Brief summary of what happened."
                            },
                            # 2. 반대 증거 강제 (악마의 변호인)
                            # 이 필드를 채우면서 모델은 자신의 확신을 낮추게 됨
                            "counter_evidence": {
                                "type": "string",
                                "description": "List reasons why the OPPOSITE verdict might be true. (e.g., if you think it's malicious, list why it could be normal)"
                            },
                            "result": {
                                "type": "integer",
                                "enum": [0, 1], # 0 또는 1만 허용 (Enum)
                                "description": "0 for Normal, 1 for Malicious"
                            },
                            "reason": {
                                "type": "string",
                                "description": "Reasoning for the classification"
                            },
                            "confidence": {
                                "type": "integer",
                                "description": "Confidence score between 0 and 100 based on evidence strength",
                                "minimum": 0,
                                "maximum": 100
                            }
                        },
                        "required": ["analysis_summary", "counter_evidence", "result", "reason", "confidence"],
                        "additionalProperties": False
                    }
                }
            }
        )

        analysis_content = response.choices[0].message.content
        parsed_json = json.loads(analysis_content)

        result_code = parsed_json.get("result") # 0 또는 1

        confidence_score = float(parsed_json.get("confidence", 0))
            
        # 튜플로 반환: (JSON결과, 신뢰도점수)
        return parsed_json, confidence_score

    except Exception as e:
        logger.error("OpenAI API 호출 중 오류 발생", exc_info=True)
        return None, 0.0

def process_messages(records):
    """SQS 메시지 처리 및 Elasticsearch bulk indexing"""
    failed_message_ids = []
    actions_to_index = []

    KST = timezone(timedelta(hours=9))
    current_datetime_str = datetime.now(tz=KST).strftime("%Y.%m.%d_%H")
    # dest_index = f"planit-edr-ai-analyzed-nmal5-{current_datetime_str}"
    dest_index = "planit-llm-malicious"
    analysis_time = datetime.now().isoformat() # 일관된 분석 시간을 위해 미리 정의

    for record in records:
        payload = None
        unique_id = "N/A" # 오류 로킹을 위해 ID 변수 미리 선언 

        try:
            payload = json.loads(record['body'])
            unique_id = payload.get('UniqueID')
            source_index = payload.get('SourceIndex')

            if not unique_id or not source_index:
                logger.warning(f"Message missing UniqueID or SourceIndex. Skipping.")
                continue

            # ES 문서 가져오기
            doc = fetch_document_with_retry(source_index, unique_id)
            if not doc:
                raise Exception(f"Failed to fetch document '{unique_id}' from ES")
            doc_source = doc['_source']

            # 1.5 시간적 맥락을 위해 주변 이벤트 가져오기
            event_hostname = doc_source.get("HostName")
            event_timestamp = doc_source.get("@timestamp") # '@timestamp' 필드 사용

            # Syslog 가져옴
            related_events_index_pattern = "edr-syslog-fixed*"

            related_events = fetch_related_events(
                es_client=es_client,
                hostname=event_hostname,
                timestamp_str=event_timestamp,
                main_event_id=unique_id,
                index_pattern=related_events_index_pattern,
                window_seconds=60, # 전후 60초
                size=50 # 최대 50개
            )


            # OpenAI 분석
            analysis_result, confidence_score = call_openai_api(openai_client, doc_source, related_events)

            if analysis_result:

                # --- AI 정확도 검증 코드 시작 ---

                # 1. AI 예측값(0/1)을 문자열("normal"/"malicious")로 변환
                ai_prediction_code = analysis_result.get("result") # 0 or 1 or None
                ai_prediction_str = None
                if ai_prediction_code == 0:
                    ai_prediction_str = "normal"
                elif ai_prediction_code == 1:
                    ai_prediction_str = "malicious"

                # 2. Ground Truth (정답) 가져오기 
                threat_label = doc_source.get("threat_label", {})
                ground_truth = threat_label.get("verdict") if isinstance(threat_label, dict) else None

                # 3. AI 예측과 Ground Truth 비교
                is_correct = None
                if ai_prediction_str is not None and ground_truth is not None:
                    is_correct = (ai_prediction_str == str(ground_truth).lower()) 

                # 3. 로그 상세 출력
                is_correct_str = "N/A (Missing Data)"
                if is_correct is True:
                    is_correct_str = "✅ CORRECT"
                elif is_correct is False:
                    is_correct_str = "❌ INCORRECT"
                
                logger.info(
                    f"AI Accuracy Check [ID: {unique_id}]: "
                    f"Prediction='{ai_prediction_str}' (Code: {ai_prediction_code}), "
                    f"GroundTruth='{ground_truth}'. "
                    f"Result: {is_correct_str}"
                )

                # --- AI 정확도 검증 코드 끝 ---

                # 4. 분석 결과를 doc_source에 저장 (정확도 필드 포함)
                doc_source['ai_analysis'] = {
                    "result": ai_prediction_str,
                    "result_code": ai_prediction_code, 
                    "reason": analysis_result.get("reason"),
                    "confidence": confidence_score, # 계산된 신뢰도 저장 
                    "analyzed_at": analysis_time,
                    "context_events_count": len(related_events), # 디버깅을 위해 추가
                    "accuracy": { # 정확도 결과 필드 추가
                        "ground_truth_verdict": ground_truth,
                        "is_correct": is_correct
                    }
                }

                actions_to_index.append({
                    "_op_type": "index",
                    "_index": dest_index,
                    "_id": unique_id,
                    "_source": doc_source
                })
            else:
                raise Exception(f"AI analysis result was None for UniqueID '{unique_id}'")

        except Exception as e:
            # unique_id 변수가 try 블록 초기에 할당되어 오류 로그에 ID 포함 가능
            logger.error(f"Failed to process message UniqueID '{unique_id}': {e}", exc_info=True)
            failed_message_ids.append(record['messageId'])

    # Bulk 인덱싱
    batch_size = 50
    for i in range(0, len(actions_to_index), batch_size):
        batch = actions_to_index[i:i+batch_size]
        try:
            success, failed = helpers.bulk(
                es_client,
                batch,
                raise_on_error=False,
                stats_only=True
            )
            logger.info(f"Bulk batch: {success} successful, {failed} failed")
        except (ConnectionTimeout, ConnectionError, TransportError) as e:
            logger.error(f"Bulk indexing network error: {e}", exc_info=True)
            failed_message_ids.extend([r['_id'] for r in batch])
        except Exception as e:
            logger.error(f"Bulk indexing unexpected error: {e}", exc_info=True)
            failed_message_ids.extend([r['_id'] for r in batch])

    return failed_message_ids

# ---- Lambda 핸들러(주변 이벤트 검색 로직 추가) ----
def lambda_handler(event, context):
    try:
        initialize_clients()
        failed_ids = process_messages(event.get('Records', []))
        return {
            'batchItemFailures': [{'itemIdentifier': msg_id} for msg_id in failed_ids]
        }
    except Exception as e:
        logger.error(f"Lambda handler failed: {e}", exc_info=True)
        raise e