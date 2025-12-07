import json
from aws_clients import initialize_clients
from ticket_utils import get_or_create_vector, find_similar_tickets, save_ticket_vector

def handle_search(ai_group_id):
    """ [/search] 검색 요청 처리 로직 """
    print(f"Handling SEARCH request for ID: {ai_group_id}")
    
    # 1. 벡터 조회 (없으면 생성만 하고 저장은 안 함)
    query_vector = get_or_create_vector(ai_group_id)
    
    if not query_vector:
        return {
            "statusCode": 404, 
            "body": json.dumps({"error": f"Ticket ID '{ai_group_id}' not found or cannot be vectorized."})
        }

    # 2. 유사도 검색
    results = find_similar_tickets(ai_group_id, query_vector)
    
    if results:
        return {
            "statusCode": 200, 
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(results)
        }
    else:
        return {
            "statusCode": 404, 
            "body": json.dumps({"message": "No similar tickets found."})
        }

def handle_save(ai_group_id):
    """ [/save] 저장 요청 처리 로직 """
    print(f"Handling SAVE request for ID: {ai_group_id}")
    
    # 1. 저장 함수 실행
    success, msg = save_ticket_vector(ai_group_id)
    
    if success:
        return {
            "statusCode": 200, 
            "body": json.dumps({
                "message": "Successfully saved vector.", 
                "ai_group_id": ai_group_id
            })
        }
    else:
        # 원본 데이터 없음(404)과 그 외 에러(500) 구분
        status_code = 404 if "not found" in msg else 500
        return {
            "statusCode": status_code, 
            "body": json.dumps({"error": msg})
        }

def lambda_handler(event, context):
    """
    메인 핸들러: API Gateway의 경로(resource)에 따라 분기 처리
    """
    
    # 1. 클라이언트 초기
    try:
        initialize_clients()
    except Exception as e:
        print(f"Server Init Error: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": "Server Init Error"})}

    # 2. 공통 입력 파싱 (Body에서 ai_group_id 추출)
    try:
        if 'body' in event:
            body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
        else:
            body = event
            
        query_group_id = body.get('ai_group_id')
        
        if not query_group_id:
            raise ValueError("ai_group_id is required")
            
        # 숫자 변환 (EC2 Elastic 필드 타입에 맞춤)
        query_group_id = int(query_group_id)
        
    except Exception as e:
        return {"statusCode": 400, "body": json.dumps({"error": f"Bad Request: {str(e)}"})}

    # 3. 라우팅 (Resource Path 확인)
    # API Gateway에서 설정한 리소스 경로가 event['resource']에 들어옴 (예: "/search", "/save")
    resource_path = event.get('resource', '')

    print(f"Requested Resource Path: {resource_path}")

    if resource_path == '/search':
        return handle_search(query_group_id)
    
    elif resource_path == '/save':
        return handle_save(query_group_id)
    
    else:
        # 경로가 없거나(직접 테스트 시) 매칭되지 않는 경우
        # 기본 동작을 설정하거나 에러를 반환 여기서는 /search를 기본으로 둠
        print(f"Unknown resource: '{resource_path}'. Defaulting to SEARCH.")
        return handle_search(query_group_id)