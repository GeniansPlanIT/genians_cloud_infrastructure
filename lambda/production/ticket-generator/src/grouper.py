import json
from clients import get_openai_client
import config

def group_events_by_context(predicted_case_id, incoming_events, reference_story):
    """
    RAG: Reference Story를 참고하여 Incoming Events를 Ticket 단위로 그룹핑
    """
    # LLM에 보낼 입력 데이터 간소화 (토큰 절약)
    target_events_text = []
    for ev in incoming_events:
        # 이벤트 식별을 위해 임시 ID나 인덱스 사용
        ev_idx = ev.get('tmp_index') 
        time_val = ev.get('EventTime')
        host = ev.get('HostName')
        summary = ev.get('generated_summary')
        target_events_text.append(f"- [ID:{ev_idx}] [Time:{time_val}] [Host:{host}] {summary}")

    target_text = "\n".join(target_events_text)

    system_prompt = (
        "당신은 보안 관제 센터(SOC)의 AI 분석가입니다. "
        "과거의 공격 사례(Reference)를 참고하여, 현재 탐지된 이벤트 목록(Targets)을 "
        "개별 사건(Ticket) 단위로 그룹핑해야 합니다."
    )

    user_prompt = f"""
    [지시 사항]
    1. **Reference Story**는 과거에 분석된 공격의 전체 흐름입니다.
    2. **Target Events**는 방금 유입된 이벤트들입니다. 이들은 Reference와 유사한 패턴을 보이지만, 서로 다른 시간대나 다른 호스트에서 발생한 별개의 사건일 수 있습니다.
    3. Target Events를 분석하여 **'같은 공격 흐름'**에 속하는 것끼리 묶어주세요.
    4. **그룹핑 기준**:
       - **시간적 근접성**: 공격 단계(Step 1->2)는 짧은 시간 내에 연속해서 일어납니다. 시간이 많이 차이나면 다른 티켓으로 분리하세요.
       - **호스트 일치**: 동일한 공격 흐름은 보통 같은 호스트(Host) 내에서 일어납니다.
       - **맥락적 연결**: Reference Story의 순서(Step 1 -> Step 2)와 일치하는 흐름을 찾으세요.
       - Reference Story와 Target Event가 명확하게 흐름이 연결되지 않으면 절대 같은 Ticket으로 묶지 마세요.
        - 유사도는 낮은데 summary만 비슷한 경우 '단독 Ticket'으로 분리하세요.
        - 공격 흐름과 무관한 이벤트는 모두 별도 Ticket으로 둡니다.

    [Reference Story (정답지)]
    {reference_story}

    [Target Events (분석 대상)]
    {target_text}

    [출력 형식 (JSON Only)]
    반드시 아래 JSON 리스트 형식으로만 출력하세요. 설명은 생략합니다.
    [
      {{
        "ticket_title": "공격 유형 요약 (예: 계정 탈취 시도 #1)",
        "host": "공격 대상 호스트",
        "event_ids": [1, 2],  <-- Target Events의 ID 리스트
        "reason": "이유 설명 (시간적 연속성 등)"
      }},
      ...
    ]
    """

    client = get_openai_client()
    resp = client.chat.completions.create(
        model=config.OPENAI_MODEL_ID,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        response_format={"type": "json_object"} # JSON 모드 강제
    )
    
    try:
        result = json.loads(resp.choices[0].message.content)
        # 반환 형식이 {"tickets": [...]} 일수도 있고 리스트일 수도 있음. 보정 필요.
        if "tickets" in result:
            return result["tickets"]
        return result
    except:
        return []