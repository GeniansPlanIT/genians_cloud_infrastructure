import json
import config
from clients import get_bedrock_client


def get_embedding(text: str):
    """
    AWS Bedrock Titan Embedding 호출
    """
    body = json.dumps({
        "inputText": text,
        "dimensions": 256,
        "normalize": True,
    })

    client = get_bedrock_client()
    resp = client.invoke_model(
        modelId=config.BEDROCK_EMBED_MODEL_ID,
        body=body,
        contentType="application/json",
        accept="application/json",
    )

    payload = json.loads(resp["body"].read())
    return payload["embedding"]
