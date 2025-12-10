import os
import json
import base64
import gzip
import urllib3
import boto3 

http = urllib3.PoolManager()
ssm = boto3.client('ssm')

def get_slack_webhook_url():
    response = ssm.get_parameter(
        Name='/planit/notification/slack/api/webhook_url',
        WithDecryption=True  # SecureString이면 True 필수
    )
    return response['Parameter']['Value']

def send_slack(text):
    webhook_url = get_slack_webhook_url()
    payload = {
        "text": text,
        # "username": "API Lambda Alert Bot",
        # "icon_emoji": ":rotating_light:"
    }
    http.request(
        "POST",
        webhook_url,
        body=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"}
    )

def lambda_handler(event, context):
    # Case 1: CloudWatch Logs (기존 로직)
    if "awslogs" in event:
        data = event["awslogs"]["data"]
        payload = json.loads(gzip.decompress(base64.b64decode(data)))
        
        log_group = payload.get("logGroup")
        log_stream = payload.get("logStream")
        
        for e in payload.get("logEvents", []):
            msg = e.get("message", "")
            text = f":rotating_light: *Lambda Log Alert*\n• Group: `{log_group}`\n• Stream: `{log_stream}`\n```\n{msg[:3000]}\n```"
            send_slack(text)

    # Case 2: Step Functions 직접 호출 (새로 추가된 로직)
    else:
        error = event.get("Error", "Unknown Error")
        cause = event.get("Cause", "No specific cause provided.")
        execution_id = event.get("ExecutionId", "Unknown Execution")
        
        # 에러 메시지가 JSON 형태인 경우 파싱 시도 (가독성 향상)
        try:
            cause_json = json.loads(cause)
            cause_formatted = json.dumps(cause_json, indent=2, ensure_ascii=False)
        except:
            cause_formatted = cause

        text = f":rotating_light: *Step Functions Workflow Failed*\n\n*Error*: `{error}`\n*Execution ID*: `{execution_id}`\n\n*Details*:\n```\n{cause_formatted[:3000]}\n```"
        send_slack(text)

    return {"statusCode": 200}
