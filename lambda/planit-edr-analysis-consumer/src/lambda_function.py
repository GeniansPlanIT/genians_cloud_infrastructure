import os
import json
import time
import logging
from datetime import datetime, timedelta, timezone

import boto3
from elasticsearch import Elasticsearch, ConnectionError, TransportError
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

def translate_fields_to_korean(openai_client, analysis_result):
    """
    analysis_result 안의 영어 텍스트(analysis_summary, counter_evidence, reason)를
    OpenAI API로 한국어로 번역하는 함수
    """

    text_to_translate = f"""
    아래 세 문장을 자연스러운 한국어로 번역해주세요.

    [analysis_summary]
    {analysis_result.get("analysis_summary", "")}

    [counter_evidence]
    {analysis_result.get("counter_evidence", "")}

    [reason]
    {analysis_result.get("reason", "")}

    번역 결과는 JSON 형식으로 반환하세요 (analysis_summary_ko, counter_evidence_ko, reason_ko 필드 포함).
    """

    try:
        response = openai_client.chat.completions.create(
            model="gpt-5",
            messages=[
                {"role": "system", "content": "You are a professional Korean translator specialized in cybersecurity."},
                {"role": "user", "content": text_to_translate}
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "kor_translation",
                    "strict": True,
                    "schema": {
                        "type": "object",
                        "properties": {
                            "analysis_summary_ko": {"type": "string"},
                            "counter_evidence_ko": {"type": "string"},
                            "reason_ko": {"type": "string"}
                        },
                        "required": ["analysis_summary_ko", "counter_evidence_ko", "reason_ko"],
                        "additionalProperties": False
                    }
                }
            }
        )

        return json.loads(response.choices[0].message.content)

    except Exception as e:
        logger.error(f"한국어 번역 중 오류 발생: {e}", exc_info=True)
        return {
            "analysis_summary_ko": "",
            "counter_evidence_ko": "",
            "reason_ko": ""
        }


# ---- 헬퍼 함수 ----
def get_parameter(ssm_client, name, with_decryption=True):
    try:
        response = ssm_client.get_parameter(Name=name, WithDecryption=with_decryption)
        return response['Parameter']['Value']
    except Exception as e:
        logger.error(f"SSM에서 '{name}' 파라미터를 가져오는 데 실패했습니다.", exc_info=True)
        raise e

def initialize_clients():
    """ES 및 OpenAI 클라이언트 초기화"""
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
    """ES 문서 가져오기 재시도"""
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

def clean_data_for_llm(data, placeholders=None):
    """
    데이터를 LLM에 전달하기 전에 재귀적으로 정리
    -1, "-", "" 등 "없음"을 의미하는 값들을 None (JSON 'null')으로 통일하여 토큰 절약
    """
    if placeholders is None:
        placeholders = [-1, "-", ""]

    if isinstance(data, dict):
        return {k: clean_data_for_llm(v, placeholders) for k, v in data.items()}

    if isinstance(data, list):
        return [clean_data_for_llm(item, placeholders) for item in data]

    if data in placeholders:
        return None

    return data

def fetch_related_events(es_client, hostname, timestamp_str, main_event_id, index_pattern="edr-syslog-fixed*", window_seconds=60, size=20):
    """
    주어진 호스트와 시간대를 기준으로 주변 로그(Context) 검색
    """
    if not hostname or not timestamp_str:
        logger.warning("Cannot fetch related events: HostName or @timestamp is missing from main event.")
        return []

    try:
        # 'Z' (UTC) 처리
        event_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
    except (ValueError, TypeError):
        logger.error(f"Invalid @timestamp format, cannot parse: {timestamp_str}")
        return []

    start_time = (event_time - timedelta(seconds=window_seconds)).isoformat()
    end_time = (event_time + timedelta(seconds=window_seconds)).isoformat()

    query_body = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"edr.HostName": hostname}},
                    {"range": {"@timestamp": {"gte": start_time, "lte": end_time}}}
                ],
                "filter": [
                    {
                        "bool": {
                            "should": [
                                {"match": {"edr.EventType": "process"}},
                                {"match": {"edr.EventType": "network"}},
                                {"match": {"edr.EventType": "file"}},
                                {"match": {"edr.EventType": "registry"}}
                            ],
                            "minimum_should_match": 1
                        }
                    }
                ]
            }
        },
        "sort": [{"@timestamp": {"order": "asc"}}],
        "_source": [
            "@timestamp", "edr.HostName", "edr.ProcUserID",
            "edr.EventType", "edr.EventSubType",
            "edr.ProcName", "edr.ProcPath", "edr.CmdLine",
            "edr.ParentProcName", "edr.ParentProcPath", "edr.ParentProcCmdLine",
            "edr.Direction", "edr.RemoteIP", "edr.RemotePort", "edr.DNSName",
            "edr.FileName", "edr.FilePath", "edr.RegKeyPath", "edr.RegValueName"
        ],
        "size": size
    }
    
    logger.info(f"[DEBUG] Fetching related events for host: {hostname}, time: {timestamp_str}")

    try:
        response = es_client.search(index=index_pattern, body=query_body)
        return [hit['_source'] for hit in response['hits']['hits']]
    except Exception as e:
        logger.warning(f"Failed to fetch related events: {e}")
        return []

def call_openai_api(current_openai_client, doc_source, related_events=None):
    """OpenAI API 호출 (Self-Evaluation & Context Aware & JSON Schema)"""

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

        # Persistence 관련 필드
        "RegKeyPath": detect_desc.get("RegKeyPath") or doc_source.get("RegKeyPath"),
        "RegValueName": detect_desc.get("RegValueName") or doc_source.get("RegValueName"),
        "RegValue": detect_desc.get("RegValue") or doc_source.get("RegData"),

        "RuleName": doc_source.get("RuleName"),
        "RuleID": doc_source.get("RuleID"),
        "DetectSubType": doc_source.get("DetectSubType"),
        "ThreatID": doc_source.get("ThreatID"),
        "Tactic": doc_source.get("Tactic"),
        "TacticID": doc_source.get("TacticID"),
        "Technique": doc_source.get("Technique"),
        "TechniqueID": doc_source.get("TechniqueID"),
        "ResponseInfo": doc_source.get("ResponseInfo"),
        "SuspiciousInfo": doc_source.get("SuspiciousInfo"),
        "SuspiciousInfo2": doc_source.get("SuspiciousInfo2")
    }

    # 데이터 정제 (null 처리)
    cleaned_main_event = clean_data_for_llm(main_event_context)
    cleaned_related_events = clean_data_for_llm(related_events)

    context_data = {
        "main_event": cleaned_main_event,
        "related_events_60sec_window": cleaned_related_events
    }

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
    """

    try:
        response = current_openai_client.chat.completions.create(
            model="gpt-5", # 사용하시는 모델명 유지
            messages=[
                {"role": "system", "content": "You are a top-tier cybersecurity analyst."},
                {"role": "user", "content": prompt}
            ],
            # Structured Outputs (Schema) 적용
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "threat_classification",
                    "strict": True,
                    "schema": {
                        "type": "object",
                        "properties": {
                            "analysis_summary": {"type": "string", "description": "Brief summary of what happened."},
                            "counter_evidence": {"type": "string", "description": "Reasons why the OPPOSITE verdict might be true."},
                            "result": {"type": "integer", "enum": [0, 1], "description": "0 for Normal, 1 for Malicious"},
                            "reason": {"type": "string", "description": "Reasoning for the classification"},
                            "confidence": {"type": "integer", "minimum": 0, "maximum": 100, "description": "Confidence score"}
                        },
                        "required": ["analysis_summary", "counter_evidence", "result", "reason", "confidence"],
                        "additionalProperties": False
                    }
                }
            }
        )

        analysis_content = response.choices[0].message.content
        parsed_json = json.loads(analysis_content)
        confidence_score = float(parsed_json.get("confidence", 0))
        
        return parsed_json, confidence_score

    except Exception as e:
        logger.error("OpenAI API 호출 중 오류 발생", exc_info=True)
        return None, 0.0


# ---- Lambda 핸들러 (Step Function Map State용) ----
def lambda_handler(event, context):
    """
    Step Function Map 상태에서 전달된 단일 'event' 처리
    Payload 예시: {'UniqueID': 'id-123', 'SourceIndex': 'src-idx', 'DestIndex': 'dest-idx'}
    """
    
    payload = event
    unique_id = payload.get('UniqueID')
    source_index = payload.get('SourceIndex')
    dest_index = payload.get('DestIndex')

    logger.info(f"Processing event from Step Function for UniqueID: {unique_id}")

    try:
        initialize_clients()

        if not unique_id or not source_index or not dest_index:
            logger.error(f"Message missing UniqueID, SourceIndex, or DestIndex.", extra={"payload": payload})
            raise ValueError("Message missing UniqueID, SourceIndex, or DestIndex.")

        # 1. ES 문서 가져오기
        doc = fetch_document_with_retry(source_index, unique_id)
        if not doc:
            raise Exception(f"Failed to fetch document '{unique_id}' from ES")
        doc_source = doc['_source']

        # 2. 주변 이벤트(Context) 가져오기 (업데이트된 부분)
        event_hostname = doc_source.get("HostName")
        event_timestamp = doc_source.get("@timestamp") # 타임스탬프 필드명 확인 필요
        
        related_events = fetch_related_events(
            es_client=es_client,
            hostname=event_hostname,
            timestamp_str=event_timestamp,
            main_event_id=unique_id,
            index_pattern="edr-syslog-fixed*", # 필요시 파라미터화 가능
            window_seconds=60,
            size=20
        )

        # 3. OpenAI 분석 (Context 포함 호출)
        analysis_result, confidence_score = call_openai_api(openai_client, doc_source, related_events)

        
        if not analysis_result:
            raise Exception(f"AI analysis result was None for UniqueID '{unique_id}'")

        # 4. 결과 파싱 및 포맷팅
        ai_prediction_code = analysis_result.get("result") # 0 or 1
        ai_prediction_str = "malicious" if ai_prediction_code == 1 else "normal"

        # # 정확도 검증 로직 (데이터에 정답셋이 있을 경우에만 동작)
        # threat_label = doc_source.get("threat_label", {})
        # ground_truth = threat_label.get("verdict") if isinstance(threat_label, dict) else None
        
        # is_correct = None
        # if ground_truth is not None:
        #     is_correct = (ai_prediction_str == str(ground_truth).lower())
        #     logger.info(f"Accuracy Check [ID: {unique_id}]: AI={ai_prediction_str}, GT={ground_truth}, Correct={is_correct}")

        # 한국어 번역
        kor_translated = translate_fields_to_korean(openai_client, analysis_result)

        # 5. 결과 Doc 구성
        # doc_source['ai_analysis'] = {
        #     "result": ai_prediction_str,
        #     "result_code": ai_prediction_code,
        #     "reason": analysis_result.get("reason"),
        #     "analysis_summary": analysis_result.get("analysis_summary"),
        #     "counter_evidence": analysis_result.get("counter_evidence"),
        #     "confidence": confidence_score,
        #     "analyzed_at": datetime.now().isoformat(),
        #     "context_events_count": len(related_events)
        # }

        doc_source['ai_analysis'] = {
            # 한국어 번역 결과
            "analysis_summary": kor_translated.get("analysis_summary_ko"),
            "counter_evidence": kor_translated.get("counter_evidence_ko"),
            "reason": kor_translated.get("reason_ko"),

            # 분류 정보
            "result": ai_prediction_str,
            "result_code": ai_prediction_code,
            "confidence": confidence_score,

            "analyzed_at": datetime.now().isoformat(),
            "context_events_count": len(related_events)
        }

        # --- 영어 원본 저장 (영어 전용 필드) ---
        doc_source['ai_analysis_eng'] = {
            "analysis_summary_eng": analysis_result.get("analysis_summary"),
            "counter_evidence_eng": analysis_result.get("counter_evidence"),
            "reason_eng": analysis_result.get("reason")
        }

        # 6. ES 저장 (단일 Index API 사용)
        # Step Function이 각 아이템을 개별 실행하므로 Bulk가 아닌 Index API 사용
        es_client.index(
            index=dest_index,
            id=unique_id,
            document=doc_source
        )

        logger.info(f"Successfully processed and indexed UniqueID: {unique_id} to {dest_index}")
        
        # SFN Map 상태 결과 반환
        return {"status": "success", "unique_id": unique_id, "prediction": ai_prediction_str}

    except Exception as e:
        logger.error(f"Failed to process message UniqueID '{unique_id}': {e}", exc_info=True)
        raise e