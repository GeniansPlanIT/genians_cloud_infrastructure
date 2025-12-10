import json
from datetime import datetime
from opensearchpy.exceptions import NotFoundError
import config
import aws_clients

def get_vector_from_bedrock(text):
    """Bedrock v2 모델을 사용하여 텍스트를 벡터로 변환"""
    body = json.dumps({
        "inputText": text,
        "dimensions": 256,
        "normalize": True
    })
    response = aws_clients.bedrock_client.invoke_model(
        body=body, modelId=config.BEDROCK_MODEL_ID,
        accept="application/json", contentType="application/json"
    )
    response_body = json.loads(response.get("body").read())
    return response_body.get("embedding")

def build_hybrid_string(events):
    """원본 이벤트 리스트에서 하이브리드 텍스트 생성"""
    if not events:
        return ""
        
    first_event = events[0]['_source']
    scenario = first_event.get("LLMScenario", "No Scenario")
    tactics = first_event.get("LLMTactics", "No Tactics")
    reasons_list = first_event.get("LLMReasons", ["No Reason"])
    reasons = ". ".join(reasons_list) if isinstance(reasons_list, list) else str(reasons_list)
    
    unique_rules = set()
    unique_cmds = set()
    unique_types = set()
    unique_classifications = set()
    
    for event in events:
        source = event['_source']
        if source.get("RuleID"): unique_rules.add(source["RuleID"])
        if source.get("DetectSubType"): unique_types.add(source["DetectSubType"])
        if source.get("SuspiciousInfo") and source["SuspiciousInfo"].get("Classification"):
            unique_classifications.add(source["SuspiciousInfo"]["Classification"])
        
        if source.get("ResponseInfo") and source["ResponseInfo"].get("detect_terminateprocess"):
            for proc in source["ResponseInfo"]["detect_terminateprocess"]:
                if proc.get("CmdLine"): unique_cmds.add(proc["CmdLine"])
                
    hybrid_text = (
        f"Scenario: {scenario}. Reasons: {reasons}. Tactics: {tactics}. "
        f"Rules: {', '.join(unique_rules)}. Commands: {', '.join(unique_cmds)}. "
        f"Types: {', '.join(unique_types | unique_classifications)}."
    )
    return hybrid_text

def fetch_events_by_id_from_elastic(query_group_id):
    """EC2 Elastic(7.x)에서 원본 티켓 조회"""
    try:
        # 7.x 구문 호환성을 위해 body 파라미터 사용
        response = aws_clients.es_read_client.search(
            index=config.SOURCE_INDEX,
            size=100,
            body={
                "query": {
                    "term": { "ai_group_id": query_group_id }
                }
            }
        )
        return response['hits']['hits']
    except Exception as e:
        print(f"EC2 Elastic 조회 실패 (ID: {query_group_id}): {e}")
        return []

def get_or_create_vector(query_group_id):
    """벡터 조회 또는 생성 (검색용)"""
    try:
        # 1. OpenSearch에서 벡터 조회 시도
        print(f"ID {query_group_id}: 벡터 조회 시도...")
        doc = aws_clients.os_client.get(index=config.TARGET_INDEX, id=str(query_group_id))
        print("벡터 조회 성공 (OpenSearch).")
        return doc['_source']['ticket_vector']
    
    except NotFoundError:
        # 2. 없으면 생성
        print(f"ID {query_group_id}: 벡터 없음. 생성 시작...")
        
        events = fetch_events_by_id_from_elastic(query_group_id)
        if not events:
            print("원본 데이터 조회 실패.")
            return None

        hybrid_text = build_hybrid_string(events)
        query_vector = get_vector_from_bedrock(hybrid_text)
        print("벡터 생성 성공 (Bedrock).")
        
        # 저장 로직은 별도 API로 분리 (사용자 요청)
        return query_vector
        
    except Exception as e:
        print(f"벡터 조회/생성 중 오류: {e}")
        return None

def find_similar_tickets(query_group_id, query_vector):
    """유사한 티켓 검색 (본인 제외, 상위 10개, 점수 0.9 이상)"""
    target_count = 10
    SCORE_THRESHOLD = 0.9

    # size를 넉넉하게 (5 + 1 = 6) 설정하여 자기 자신이 빠져도 5개가 되도록 보장
    fetch_size = target_count + 1

    knn_query = {
        "size": target_count,
        "query": {
            "bool": {
                "must": [
                    {
                        "knn": {
                            "ticket_vector": {
                                "vector": query_vector,
                                "k": fetch_size + 5
                            }
                        }
                    }
                ],
                "must_not": [
                    {"term": {"ai_group_id": query_group_id}}
                ]
            }
        },
        "_source": ["ai_group_id"]
    }

    try:
        response = aws_clients.os_client.search(index=config.TARGET_INDEX, body=knn_query)
        hits = response['hits']['hits']
        
        results = []
        for hit in hits:
            score = hit['_score']
            if score >= SCORE_THRESHOLD:
                results.append({
                    "ai_group_id": hit['_source']['ai_group_id'],
                    "score": score
                })
                
        # 결과가 5개보다 많으면 상위 5개만 잘라서 반환
        return results[:target_count]

    except Exception as e:
        print(f"k-NN 검색 실패: {e}")
        return []



def save_ticket_vector(query_group_id):
    """티켓 저장"""
    try:
        print(f"ID {query_group_id}: 원본 조회 중...")
        events = fetch_events_by_id_from_elastic(query_group_id)
        if not events:
            return False, "Original data not found."

        print(f"ID {query_group_id}: 벡터 생성 중...")
        hybrid_text = build_hybrid_string(events)
        query_vector = get_vector_from_bedrock(hybrid_text)

        print(f"ID {query_group_id}: OpenSearch 저장 중...")
        doc_body = {
            "ai_group_id": query_group_id,
            "ticket_vector": query_vector,
            "created_at": datetime.now().isoformat()
        }
        aws_clients.os_client.index(
            index=config.TARGET_INDEX, 
            id=str(query_group_id), 
            body=doc_body
        )
        return True, "Successfully saved."
    except Exception as e:
        return False, str(e)