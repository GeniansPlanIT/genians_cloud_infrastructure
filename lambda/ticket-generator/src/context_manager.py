from clients import get_opensearch_client
import config

def get_reference_story(case_id):
    """
    OpenSearch에서 특정 case_id의 전체 이벤트 시퀀스를 가져와 정렬
    """
    if case_id in ["UNKNOWN", "ERROR"]:
        return "참고할 과거 사례가 없습니다 (신규 위협 가능성)."

    os_client = get_opensearch_client()
    
    query = {
        "size": 10, # 한 시나리오당 이벤트가 10개 넘지는 않을 것으로 가정
        "query": {
            "term": {
                "case_id": case_id
            }
        },
        "sort": [
            {"event_seq": {"order": "asc"}}
        ],
        "_source": ["event_seq", "summary"]
    }
    
    resp = os_client.search(index=config.OS_INDEX, body=query)
    hits = resp['hits']['hits']
    
    if not hits:
        return "참고 데이터 없음"
        
    # 과거 사례를 하나의 스토리로 합침
    story_lines = [f"참고 사례 ID: {case_id}"]
    for hit in hits:
        seq = hit['_source']['event_seq']
        summ = hit['_source']['summary']
        story_lines.append(f"[Step {seq}] {summ}")
        
    return "\n".join(story_lines)