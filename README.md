# AI 기반 보안 운영 파이프라인(AI-Driven Security Operations Pipeline)

## 📄프로젝트 개요
**PLANIT AI Pipeline**은 EDR(Endpoint Detection and Response) 및 네트워크 로그를 수집하여 **AI(LLM) 기반으로 위협을 자동 분석하고, 연관된 보안 이벤트를 하나의 티켓으로 그룹핑**하는 자동화된 보안 관제 시스템입니다.

기존 관제 시스템의 높은 오탐(False Positive)과 단순 반복 업무를 해결하기 위해 **Context-Aware Analysis(맥락 기반 분석)**와 **RAG(Retrieval-Augmented Generation)** 기술을 도입하였습니다.

## 🗃️ 아키텍처 (Architecture)

본 프로젝트는 **AWS Serverless Architecture**를 기반으로 설계되었으며, **AWS Step Functions**를 통해 전체 데이터 파이프라인의 상태를 관리합니다.

```mermaid
graph TD
    subgraph "Data Ingestion"
        A[EDR/NAC API] -->|Lambda| B(Data Ingestor)
        B -->|Raw Data| C[(Amazon S3)]
        B -->|Indexing| D[(Elastic)]
    end

    subgraph "Orchestration & Analysis"
        E[Step Functions] -->|Trigger| F(Analysis Producer)
        F -->|Map State| G(Analysis Consumer)
        
        G -->|Fetch Context +/- 60s| D
        G -->|LLM Evaluation| H[OpenAI GPT-5]
        G -->|Result Indexing| D
    end

    subgraph "Ticket Generation (RAG)"
        I(Ticket Generator) -->|Embed Summary| J[AWS Bedrock]
        I -->|Vector Search| D
        I -->|Context Grouping| H
        I -->|Create Ticket| D
    end

    subgraph "Notification"
        E -->|On Failure/Result| K(Slack Notifier)
    end
