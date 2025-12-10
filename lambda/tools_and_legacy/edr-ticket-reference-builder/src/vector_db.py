from datetime import datetime
import config
from clients import get_opensearch_client


def save_event_vector(case_id: str, event_seq: int, summary: str, embedding, raw_event: dict):
    """
    event-level vector 저장
    문서 ID = "{case_id}_{event_seq}"
    ex) INCIDENT-20251010-41_3
    """
    doc_id = f"{case_id}_{event_seq}"

    body = {
        "case_id": case_id,
        "event_seq": event_seq,
        "summary": summary,
        "vector": embedding,
        "raw_event": raw_event,   # 필요 시 필수 필드만 골라서 저장하도록 변경 가능
        "created_at": datetime.utcnow().isoformat() + "Z",
    }

    os_client = get_opensearch_client()
    os_client.index(
        index=config.OS_INDEX,
        id=doc_id,
        body=body,
    )
    return True
