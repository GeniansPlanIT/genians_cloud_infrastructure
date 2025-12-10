import json
import time
from datetime import datetime
import concurrent.futures
from collections import defaultdict

# RAG 로직 (내부적으로 OpenSearch 사용)
from classifier import batch_classify_events
from context_manager import get_reference_story
from grouper import group_events_by_context

# I/O 로직 (Elasticsearch 사용)
from clients import get_elasticsearch_client 
import config

BATCH_SIZE = 100 # 한 번에 처리할 문서 수 

def get_next_group_id_start(es_client):
    """
    [ES] Output Index 패턴 전체에서 Max ID 조회 (EC2 로직 동일)
    """

    index_pattern = "planit-edr-ai-grouping-*"
    query = {
        "size": 0,
        "aggs": {
            "max_id": { "max": { "field": "ai_group_id" } }
        }
    }
    try:
        response = es_client.search(index=index_pattern, body=query, ignore_unavailable=True)
        max_val = response.get('aggregations', {}).get('max_id', {}).get('value')
        
        if max_val is None: 
            print("[INFO] 기존 그룹 ID가 없습니다. 1부터 시작합니다.")
            return 1
            
        next_id = int(max_val) + 1
        print(f"[INFO] 현재 최대 ID: {int(max_val)}, 다음 시작 ID: {next_id}")
        return next_id
        
    except Exception as e:
        print(f"[WARN] ES ID 조회 실패: {e}")
        return 1

def fetch_unprocessed_events(es_client, batch_id):
    """
    [ES] Input Index에서 'malicious' 이벤트 조회 
    Input: batch_id (예: "2025.11.20_14")
    """
    # 1. 인덱스 이름 구성 
    target_index_date = batch_id.split('_')[0] # "2025.11.20"
    input_index = f"planit-edr-ai-classified-{target_index_date}"
    
    print(f"[INFO] Fetching events from: {input_index} (Batch: {batch_id})")
    
    # 2. 쿼리 구성 (EC2 로직: malicious + EventDate 필터)
    query = {
        "size": BATCH_SIZE, # Lambda는 메모리 제한이 있으므로 size 제한 필요
        "query": {
            "bool": {
                "must": [
                    {"term": {"ai_analysis.result": "malicious"}}, # 악성만 조회
                    {"term": {"EventDate": batch_id}}               # 해당 시간대만 조회
                ]
            }
        },
        "sort": [
            {"HostName": {"order": "asc"}}, 
            {"EventTime": {"order": "asc"}}
        ]
    }
    
    try:
        resp = es_client.search(index=input_index, body=query, ignore_unavailable=True)
        hits = resp['hits']['hits']
        
        events = []
        for hit in hits:
            ev = hit['_source']
            ev['_id'] = hit['_id']
            events.append(ev)
            
        print(f"[INFO] Fetched {len(events)} malicious events.")
        return events
        
    except Exception as e:
        print(f"[ERROR] Fetch failed: {e}")
        return []

def bulk_index_grouped_events(es_client, tickets, start_id, batch_id):
    """
    [ES] Output Index에 티켓 저장
    """
    # 1. Output 인덱스 이름 구성 
    target_index_date = batch_id.split('_')[0]
    output_index = f"planit-edr-ai-grouping-{target_index_date}"
    
    print(f"[INFO] Saving grouped events to: {output_index}")
    
    bulk_data = []
    current_id_counter = start_id
    grouped_at = datetime.utcnow().isoformat() + "Z"
    
    for ticket in tickets:
        # 그룹 ID 할당
        group_id = current_id_counter
        current_id_counter += 1
        
        for ev in ticket.get('events', []):
            ev['ai_group_id'] = group_id
            ev['ai_grouped_at'] = grouped_at
            
            # 임시 필드 정리
            ev.pop('tmp_index', None)
            
            # _id 확보 (ES _id가 있으면 우선 사용, 없으면 UniqueID)
            doc_id = ev.pop('_id', ev.get('UniqueID'))
            
            # ES Bulk Action (Update/Index)
            action = {"index": {"_index": output_index, "_id": doc_id}}
            bulk_data.append(json.dumps(action))
            bulk_data.append(json.dumps(ev))
            
    if not bulk_data: 
        return 0, current_id_counter
    
    # Bulk 요청 실행
    body = "\n".join(bulk_data) + "\n"
    resp = es_client.bulk(body=body, index=output_index)
    
    if resp.get('errors'):
        print("[WARN] Bulk indexing 중 일부 에러 발생 (세부 로그 확인 필요)")
    
    # 실제 저장된 문서 수 (bulk_data는 메타데이터+문서 쌍이므로 나누기 2)
    saved_doc_count = len(bulk_data) // 2
    return saved_doc_count, current_id_counter

def process_single_group(args):
    case_id, group_events = args
    results = []

    try:
        # case_id별 Reference Story 로딩
        if case_id in ["UNKNOWN", "ERROR"]:
            # UNKNOWN의 경우 reference 없이 RAG 동작
            reference_story = "이 사건은 기존 공격 시나리오와 일치하지 않는 UNKNOWN 그룹입니다. \
                               Target Events 간의 시간/호스트/행위 기반으로 자동 클러스터링하세요."
        else:
            reference_story = get_reference_story(case_id)

        # LLM 그룹핑 수행 (UNKNOWN도 포함)
        grouped_results = group_events_by_context(case_id, group_events, reference_story)

        # ID → event 매핑
        event_map = {ev['tmp_index']: ev for ev in group_events}

        # grouped_results → events {…} 구성
        for ticket in grouped_results:
            ticket_events = []
            ids = ticket.get('event_ids', [])

            for eid in ids:
                if eid in event_map:
                    ticket_events.append(event_map[eid])

            ticket['events'] = ticket_events
            ticket.pop('event_ids', None)
            results.append(ticket)

    except Exception as e:
        print(f"[ERROR] Grouping error for {case_id}: {e}")
        
        # 에러 시 fallback 단독 티켓
        for ev in group_events:
            results.append({"events": [ev]})

    return results


def lambda_handler(event, context):
    es_client = get_elasticsearch_client()
    
    # Step Function에서 전달받은 파라미터 추출
    # 예: { "batch_id": "2025.11.20_14", ... }
    batch_id = event.get('batch_id')
    
    if not batch_id:
        print("[ERROR] 'batch_id' parameter is missing.")
        return {"statusCode": 400, "body": "Missing batch_id"}

    # 1. [ES] 데이터 로드
    raw_events = fetch_unprocessed_events(es_client, batch_id)
    
    if not raw_events: 
        print(f"[INFO] Batch {batch_id}: No malicious events found.")
        return {"statusCode": 200, "body": "No events"}

    # 전처리
    for idx, ev in enumerate(raw_events): ev['tmp_index'] = idx

    # 2. [OS 사용] 분류
    classified_events = batch_classify_events(raw_events)
    
    # 3. 버킷팅
    buckets = defaultdict(list)
    for ev in classified_events:
        buckets[ev.get('predicted_case_id', 'UNKNOWN')].append(ev)
    
    final_tickets = []

    # 4. [OS 사용] RAG 그룹핑
    print(f"[Step 3] RAG 그룹핑 시작 (대상 그룹 수: {len(buckets)})")
    
    group_args = [(case_id, events) for case_id, events in buckets.items()]
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = list(executor.map(process_single_group, group_args))
        
    for res_list in futures:
        final_tickets.extend(res_list)
        
    print(f"[Step 3] 병렬 그룹핑 완료. 생성된 티켓 수: {len(final_tickets)}")

    # 5. [ES] 저장
    start_id = get_next_group_id_start(es_client)
    # batch_id를 넘겨주어 Output 인덱스명을 생성하도록 함
    saved_count, last_id = bulk_index_grouped_events(es_client, final_tickets, start_id, batch_id)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "batch_id": batch_id,
            "tickets_created": len(final_tickets), 
            "docs_saved": saved_count
        })
    }