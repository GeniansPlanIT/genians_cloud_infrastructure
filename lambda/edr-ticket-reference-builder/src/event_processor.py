from typing import List, Dict
from collections import defaultdict

def assign_event_sequence(events: List[Dict]) -> List[Dict]:
    """
    라벨 데이터 입력 시,
    case_id 별로 event_seq 자동 생성 (1,2,3,...)
    단순히 EventTime(숫자/정수) 기준 정렬.
    EventTime이 문자열이면 이 부분을 datetime 파싱하도록 수정 필요.
    """
    grouped = defaultdict(list)

    for ev in events:
        case_id = ev.get("threat_label_case_id")
        if not case_id:
            continue
        grouped[case_id].append(ev)

    for case_id, ev_list in grouped.items():
        ev_list.sort(key=lambda x: x.get("EventTime", 0))
        for idx, ev in enumerate(ev_list, start=1):
            ev["event_seq"] = idx

    return events
