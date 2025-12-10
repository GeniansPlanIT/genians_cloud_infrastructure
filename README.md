# AI 기반 보안 운영 파이프라인 (AI-Driven Security Operations Pipeline)

## 📄 프로젝트 개요
> **PLANIT AI Pipeline**은 EDR(Endpoint Detection and Response) 데이터를 수집하여 **AI(LLM) 기반으로 위협을 자동 분석하고, 연관된 보안 이벤트를 하나의 티켓으로 그룹핑**하는 자동화된 보안 관제 시스템입니다.

기존 EDR 시스템의 높은 오탐(False Positive)과 단순 반복 업무를 해결하기 위해 **맥락 기반 분석**과 **RAG(Retrieval-Augmented Generation)** 기술을 도입하였습니다.

## 🗃️ 시스템 워크플로우 
<div align="center">
  <img width="400" alt="stepfunctions_graph" src="https://github.com/user-attachments/assets/9e612c86-3e77-40bd-9b00-b6b1d15869d4" />
  <p><em>[그림 1] AWS Step Functions 전체 워크 플로우</em></p>
</div>
본 프로젝트는 **AWS Serverless Architecture**를 기반으로 설계되었으며, **AWS Step Functions**를 통해 전체 데이터 파이프라인의 상태를 관리합니다.

## 🎯 핵심 기능

### 1. 맥락 기반 위협 분석
단일 위협 이벤트만 보고 판단하지 않고, 해당 이벤트 발생 **전후 60초 간의 주변 Syslog**를 함께 수집하여 AI에게 전달합니다.
- **Logic**: `planit-edr-analysis-consumer`
- **Process**: 메인 이벤트 발생 시, 동일 호스트의 프로세스, 네트워크, 파일, 레지스트리 행위 로그를 조회하여 LLM이 "정상 관리자 행위"인지 "악성 공격"인지 판단하도록 유도합니다.

### 2. RAG 기반 자동 티켓 그룹핑
단발성 경보를 쏟아내는 것이 아니라, 연관된 공격 흐름을 묶어 하나의 "**Incident Ticket**"으로 생성합니다.
- **Logic**: `ticket-generator`
- **Process**:
  1. 각 이벤트의 행위를 요약하고 임베딩(Embedding) 생성 (AWS Bedrock Titan)
  2. Vector DB(OpenSearch)에서 과거 유사 사례(Reference Story) 검색
  3. LLM이 유사도와 공격 시나리오를 기반으로 이벤트를 클러스터링하여 티켓 생성

### 3. 유사 티켓 검색
관제 요원이 현재 분석 중인 티켓과 **가장 유사했던 과거의 공격 사례**를 즉시 조회할 수 있는 기능을 제공하여 대응 속도를 높입니다.
- **Logic**: `find-similar-ticket`

### 4. Serverless Event Pipeline
- **AWS Step Functions**를 사용하여 대량의 데이터 처리 흐름을 시각화하고 에러 핸들링(Retry/Catch)을 자동화했습니다.
- **Map State**를 활용하여 수백 개의 이벤트를 병렬로 동시에 AI 분석 처리합니다.

## 🪜 기술적 의사결정 및 연구 과정
> 이 프로젝트의 핵심 과제는 "서로 다른 시간에 발생한 개별 로그들을 어떻게 하나의 공격 시나리오(티켓)로 묶을 것인가?"였습니다. 이를 해결하기 위해 두 가지 접근 방식을 시도했습니다.

### 시도 1: SageMaker를 이용한 자체 모델 학습
* **목표**: 공격 패턴을 벡터화하여 유사도 기반으로 클러스터링하는 딥러닝 모델 개발
* **구현 (`sagemaker/` 폴더)**:
    * **Advanced Feature Engineering**:
        * `Text`: `CmdLine`, `ParentCmdLine`에 대해 TF-IDF를 적용하여 명령어 패턴 추출
        * `Sequential Path`: `ProcPath`를 경로 단위로 토큰화한 후 LSTM을 통과시켜 64차원 임베딩 생성
        * `Temporal`: 이벤트 간 발생 시간 차이를 계산하고 Log Scaling & Standard Scaling을 적용하여 시간적 연관성 학습 유도
        * `Context`: `MITRE ATT&CK` 태그(Tactic/Technique)를 Multi-label Binarizer로 인코딩하고, 프로세스 트리(부모-자식 관계) 정보를 피처화
    * **Model Architecture**:
        * **Siamese Network (샴 네트워크)**: 두 이벤트 간의 유사도를 학습하기 위해 샴 구조 채택
        * **ResNet-style MLP Backbone**: Residual Block(잔차 연결) 적용
    * **Training Strategy**:
        * **Cosine Embedding Loss**: 고차원 벡터 공간에서 유클리드 거리보다 효과적인 코사인 유사도 기반 손실 함수를 사용
        * **Early Stopping & Scheduler**: `ReduceLROnPlateau`와 조기 종료 기법을 도입하여 과적합 방지
    * **Inference & Grouping**:
        * 학습된 임베딩 벡터들에 대해 Agglomerative Clustering (계층적 군집화)를 적용하여 자동으로 티켓을 생성
* **한계 및 전환 사유**:
    * 같은 공격 시나리오끼리는 잘 묶이나, 맥락에 따라 공격 시나리오 내에서 티켓으로 세밀하게 분리하지 못하는 문제 발생
    * 학습하지 않은 신종 공격 패턴이나 정상 행위가 섞인 노이즈 데이터에 대해 벡터 거리가 모호해지는 현상 발생
    * 새로운 공격 패턴이 나올 때마다 재학습 해야 하는 비용 부담으로 인해, 일반화 성능이 뛰어난 LLM 기반 RAG 방식으로 전환 결정

### 시도 2: RAG 기반의 LLM 그룹핑 (최종 채택)
* **전환 이유**: 일반화 성능 문제 해결 필요
* **구현 (`lambda/ticket-generator`)**:
    * **Embedder**: AWS Bedrock Titan을 사용하여 이벤트 요약문의 임베딩 생성
    * **Retrieval**: OpenSearch k-NN 검색으로 과거 유사 공격 사례(Reference Story) 조회
    * **Grouping**: LLM(GPT)이 과거 사례와 현재 이벤트를 비교하여 논리적 연관성을 판단
* **결과**: RAG 방식이 새로운 위협에 대해 훨씬 유연하고 정확한 그룹핑 성능을 보여 최종 프로덕션에 채택되었습니다.

## 🛠️ 기술 스택

| Category | Service / Tool | Description |
| :--- | :--- | :--- |
| **Compute** | AWS Lambda | Python 기반의 데이터 수집, AI 분석, 티켓 생성 로직 수행 |
| **Orchestration** | AWS Step Functions | 순차적/병렬적 워크플로우 관리 및 에러 핸들링 |
| **Database** | Amazon OpenSearch & ES | 로그 저장, 벡터 검색(KNN), 키워드 검색 |
| **Storage** | Amazon S3 | Raw 데이터 영구 보관 |
| **AI / ML** | OpenAI API | 위협 분석(Classification) 및 상황 요약(Summarization) |
| **Embedding** | AWS Bedrock | 텍스트 임베딩(Titan Embedding v2) 생성 |

## 📂 디렉토리 구조

```bash
.
├── api_gateway/            # API Gateway 설정
├── step_functions/         # 데이터 파이프라인 워크플로우 정의
├── lambda/
│   ├── production/       # [실제 서비스용]
│   │   ├── edr-to-elasticsearch/   # EDR 데이터 수집 및 1차 전처리
│   │   ├── nac-to-elasticsearch/   # NAC 데이터 수집
│   │   ├── planit-edr-analysis-*/  # AI 악성/정상 분석 로직 (Producer/Consumer)
│   │   ├── ticket-generator/       # RAG 기반 티켓 그룹핑
│   │   ├── find-similar-ticket/    # 유사 티켓 검색 API (벡터 검색)
│   │   └── slack-notifier/         # 에러 발생 시 Slack 알림 전송
│   │
│   └── tools_and_legacy/   # [도구 및 레거시]
│       ├── collect_edr_data_for_ai/# AI 학습 데이터 수집용 도구
│       ├── edr-ticket-reference-*/ # RAG용 레퍼런스 데이터 빌더
│       ├── *-event-ingestor/       # 초기 버전의 데이터 수집기
│       ├── process_ai_single_*/    # (Deprecated) SQS 기반 분석 로직
│       └── start-llm-malicious-*/  # (Deprecated) SQS 분석 위협 이벤트 분배
│
└── sagemaker/              # [AI 모델 연구 기록]
```

## 💡 프로젝트 상세 로직
**데이터 파이프라인 흐름 (AWS EventBridge Trigger)**

1. **Ingestion**: `edr-to-elasticsearch`가 API에서 데이터를 가져와 S3에 저장하고 ES에 1차 색인
2. **Analysis**: `consumer` 람다가 Step Functions의 Map State를 통해 병렬 실행. 주변 로그를 조회하여 위협 여부 판별
3. **Grouping**: `ticket-generator`가 'Malicious'로 판별된 이벤트들을 벡터 유사도 기반으로 묶어 최종 사고(Ticket) 생성
