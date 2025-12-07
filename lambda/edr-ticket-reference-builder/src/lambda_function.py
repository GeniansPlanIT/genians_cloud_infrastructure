import json
import concurrent.futures
import config
from clients import get_s3_client
from event_processor import assign_event_sequence
from summarizer import build_training_summary
from embedder import get_embedding
from vector_db import save_event_vector

# 한 번에 동시에 처리할 스레드 개수 (너무 높으면 OpenAI Rate Limit에 걸릴 수 있음)
MAX_WORKERS = 5 

def process_single_event(ev):
    """
    하나의 이벤트를 처리하는 단위 함수 (스레드에서 실행됨)
    """
    try:
        case_id = ev.get("threat_label_case_id")
        seq = ev.get("event_seq")
        
        if not case_id:
            return 0 # 실패 또는 건너뜀

        # 1. 요약
        summary = build_training_summary(ev)
        
        # 2. 임베딩
        emb = get_embedding(summary)
        
        # 3. 저장
        save_event_vector(case_id, seq, summary, emb, ev)
        
        print(f"[SUCCESS] {case_id}_{seq} 처리 완료")
        return 1
        
    except Exception as e:
        print(f"[ERROR] 처리 중 실패: {e}")
        return 0

def lambda_handler(event, context):
    # 0) 환경변수 체크 및 인덱스 초기화
    if not config.S3_BUCKET:
        return {"statusCode": 500, "body": "S3_BUCKET error"}
    
    # try:
    #     init_collection()
    # except Exception as e:
    #     print(f"[WARN] Index init warning: {e}")

    s3 = get_s3_client()

    # 1) 데이터 로드
    print("데이터 로드 중...")
    obj = s3.get_object(Bucket=config.S3_BUCKET, Key=config.S3_KEY)
    events = json.loads(obj["Body"].read().decode("utf-8"))
    
    # 2) 시퀀스 할당
    labeled_events = assign_event_sequence(events)
    total_count = len(labeled_events)
    print(f"총 {total_count}개 이벤트 병렬 처리 시작 (Workers: {MAX_WORKERS})")

    success_count = 0

    # 3) 병렬 처리 핵심 로직
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # 리스트 내 모든 이벤트에 대해 process_single_event 함수를 병렬 실행
        results = list(executor.map(process_single_event, labeled_events))
        
    success_count = sum(results)

    print(f"총 {success_count}/{total_count}개 처리 완료.")

    return {
        "statusCode": 200,
        "body": json.dumps({"saved_events": success_count})
    }

# import json
# import config
# from clients import get_s3_client
# from event_processor import assign_event_sequence
# from summarizer import build_training_summary
# from embedder import get_embedding
# from vector_db import save_event_vector


# def lambda_handler(event, context):
#     """
#     - S3에 올라가 있는 raw_data.json을 읽어서
#     - case_id별 event_seq 부여
#     - 각 이벤트에 대해 요약 생성 (OpenAI)
#     - 임베딩 생성 (Bedrock)
#     - OpenSearch에 event-level vector 저장
#     """

#     # 0) 필수 환경변수 체크
#     if not config.S3_BUCKET:
#         return {
#             "statusCode": 500,
#             "body": json.dumps({"error": "S3_BUCKET 환경변수가 설정되어 있지 않습니다."}),
#         }

#     s3 = get_s3_client()

#     # 1) S3에서 JSON 파일 읽기
#     print(f"S3에서 라벨 데이터 로드: bucket={config.S3_BUCKET}, key={config.S3_KEY}")
#     obj = s3.get_object(Bucket=config.S3_BUCKET, Key=config.S3_KEY)
#     raw_body = obj["Body"].read().decode("utf-8")
#     events = json.loads(raw_body)

#     if not isinstance(events, list):
#         return {
#             "statusCode": 400,
#             "body": json.dumps({"error": "raw_data.json 형식은 [ {...}, {...} ] 리스트여야 합니다."}),
#         }

#     print(f"총 {len(events)}개의 이벤트 처리 시작")

#     # 2) case_id별 event_seq 부여
#     labeled_events = assign_event_sequence(events)

#     saved = 0

#     # 3) 각 이벤트 → summary → embedding → OpenSearch 저장
#     for ev in labeled_events:
#         case_id = ev.get("threat_label_case_id")
#         seq = ev.get("event_seq")

#         if not case_id:
#             print("threat_label_case_id 없음 → skip")
#             continue

#         print(f"[{case_id} / seq {seq}] Summary 생성 중...")
#         summary = build_training_summary(ev)
        

#         print(f"[{case_id} / seq {seq}] Embedding 생성 중...")
#         emb = get_embedding(summary)

#         print(f"[{case_id} / seq {seq}] Vector DB 저장 중...")
#         save_event_vector(case_id, seq, summary, emb, ev)

#         saved += 1

#     print(f"총 {saved}개의 이벤트 벡터가 저장되었습니다.")

#     return {
#         "statusCode": 200,
#         "body": json.dumps(
#             {
#                 "message": "Label Event-level Vector DB 생성 완료",
#                 "saved_events": saved,
#             }
#         ),
#     }
