import json
import config
from clients import get_openai_client

def clean_event_data(event: dict) -> dict:
    """분석에 불필요한 null 값이나 빈 문자열 제거"""
    cleaned = {}
    for k, v in event.items():
        if v is not None and v != "":
            # 재귀적으로 내부 리스트나 딕셔너리도 청소하면 좋지만, 
            # 1단계만 걸러내도 충분히 효과적입니다.
            cleaned[k] = v
    return cleaned

def build_training_summary(event: dict) -> str:
    """
    OpenAI (GPT) 를 사용하여 label-guided 공격 요약 생성
    → 같은 case_id 내 이벤트들이 비슷한 표현 공간에 위치하게 유도
    """
    case_id = event.get("threat_label_case_id", "UNKNOWN_CASE")
    event_seq = event.get("event_seq", 0)
    scenario = event.get("threat_label_scenario", "N/A")

    cleaned_event = clean_event_data(event)


    system_prompt = (
        "당신은 사이버 보안 위협 헌팅 전문가입니다. "
        "주어진 로그를 분석하여, 향후 유사한 공격을 탐지할 수 있도록 '일반화된 공격 패턴(Generalized Attack Pattern)'을 추출해야 합니다. "
        "특정 사용자 이름, 랜덤한 해시값, 임시 경로 등 가변적인 값은 제거하거나 추상화된 태그로 변환하세요."
    )

    user_prompt = f"""
    아래 이벤트를 분석하여 벡터 유사도 검색에 최적화된 **'공격 행위 요약문'**을 작성하세요.

    [치환 규칙 - 반드시 준수할 것]
    1. 사용자 이름, 호스트명 → {{User}}, {{Host}} 로 변경
    2. 구체적인 파일 해시(SHA256 등) → {{Malware_Hash}} 로 변경 (단, cmd.exe, powershell.exe 등 시스템 파일명은 유지)
    3. 구체적인 IP 주소 → {{External_IP}} 또는 {{Local_IP}} 로 변경
    4. 랜덤한 폴더명이나 임시 ID → {{Random_ID}} 로 변경
    5. 날짜/시간 → 삭제
    6. 경로 내에 '랜섬', '악성', '테스트' 등 모의 훈련용 키워드가 있거나 '(1)' 같은 불필요한 접미사가 있다면 삭제하거나 {{Suspicious_Folder}}로 치환.

    [요약 포함 내용]
    1. **MITRE ATT&CK**: 반드시 아래 형식을 정확히 따를 것 (여러 개일 경우 줄바꿈).
       - 형식: **[Tactic: <Tactic Name> (<ID>)] [Technique: <Technique Name> (<ID>)]**
       - 예시: [Tactic: Execution (TA0002)] [Technique: Command and Scripting Interpreter (T1059)]
    2. **행위(Action)**: 무엇이 무엇을 실행했는가? (부모-자식 관계 중심)
    3. **도구(Tool)**: 사용된 윈도우 유틸리티나 스크립트 도구 (예: PowerShell, bitsadmin, DllHost)
    4. **의심스러운 특징**: (예: 다운로드 폴더에서 실행, 인코딩된 커맨드 사용)

    *주의: '이 이벤트는 Case ID ...의 1단계입니다' 같은 메타데이터 설명은 요약문에 절대 포함하지 마세요. 오직 공격 행위 자체만 묘사하세요.*

    [이벤트 JSON]
    {json.dumps(cleaned_event, indent=2, ensure_ascii=False)}

    [참고 시나리오 (힌트)]
    {scenario}
    """

    client = get_openai_client()

    resp = client.chat.completions.create(
        model=config.OPENAI_MODEL_ID,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        # max_completion_tokens=1000,
    )

    summary = resp.choices[0].message.content.strip()

    print(f"[DEBUG] OpenAI Full Response: {resp.choices[0]}")

    print(json.dumps({
        "event": "summary_generated",
        "case_id": case_id,
        "event_seq": event_seq,
        "summary": summary
        }, ensure_ascii=False)
    )

    return summary
