import concurrent.futures
from summarizer import build_training_summary
from embedder import get_embedding
from clients import get_opensearch_client
import config

# 유사도 임계값 (이 점수보다 낮으면 'Unknown'으로 분류)
SIMILARITY_THRESHOLD = 0.75

def search_similar_case(vector):
    """
    OpenSearch에서 가장 유사한 벡터(Case) 1개를 검색
    """
    os_client = get_opensearch_client()
    query = {
        "size": 1,
        "query": {
            "knn": {
                "vector": {
                    "vector": vector,
                    "k": 5
                }
            }
        },
        "_source": ["case_id", "summary", "threat_label_scenario"]
    }
    
    response = os_client.search(index=config.OS_INDEX, body=query)
    hits = response['hits']['hits']
    
    if not hits:
        return None, 0.0
        
    top_hit = hits[0]
    score = top_hit['_score']
    case_id = top_hit['_source']['case_id']
    
    return case_id, score

def process_classification(event):

    CASE_THRESHOLDS = {
    "INCIDENT-20251010-45": 0.90,  # PSTools
    "INCIDENT-20251003-024": 0.88, # Ransomware
    }

    DEFAULT_THRESHOLD = 0.80

    """
    단일 이벤트에 대한 요약 -> 임베딩 -> 검색 -> 라벨링 수행
    """
    try:
        # 1. 요약 생성 
        summary = build_training_summary(event)
        
        # 2. 임베딩 생성
        vector = get_embedding(summary)
        
        # 3. 벡터 DB 검색
        case_id, score = search_similar_case(vector)

        # 검색 실패 처리
        if case_id is None:
            event['predicted_case_id'] = "UNKNOWN"
            event['similarity_score'] = 0.0
            event['generated_summary'] = "[Unknown Activity Pattern]\n" + summary
            return event
        
        # 4. case_id별 threshold 설정 
        threshold = CASE_THRESHOLDS.get(case_id, DEFAULT_THRESHOLD)

        # 5. 결과 할당
        if score >= threshold:
            event['predicted_case_id'] = case_id
            event['similarity_score'] = score
            event['generated_summary'] = summary # RAG 단계에서 재사용
        else:
            event['predicted_case_id'] = "UNKNOWN"
            event['similarity_score'] = score
            event['generated_summary'] = summary
            
        return event
    except Exception as e:
        print(f"[ERROR] Classification failed: {e}")
        event['predicted_case_id'] = "ERROR"
        return event

def batch_classify_events(events):
    """
    ThreadPool을 사용하여 100개 이벤트를 병렬로 분류
    """
    # Lambda vCPU 개수에 맞춰 워커 수 조절 (보통 5~10 적절)
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(process_classification, events))
    return results