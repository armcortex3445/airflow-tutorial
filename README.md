# Apache Airflow 실습 프로젝트

Apache Airflow 3.x로 간단한 ETL 파이프라인(DAG)을 작성하고 로컬에서 실행해 보는 실습 프로젝트입니다.

## 목차

- [Apache Airflow 실습 프로젝트](#apache-airflow-실습-프로젝트)
  - [목차](#목차)
  - [1. Airflow란](#1-airflow란)
    - [사용 목적](#사용-목적)
    - [활용 예시](#활용-예시)
  - [2. 핵심 개념](#2-핵심-개념)
  - [3. 아키텍처](#3-아키텍처)
  - [4. 프로젝트 구조](#4-프로젝트-구조)
  - [5. 환경 구성](#5-환경-구성)
  - [6. 실행 방법](#6-실행-방법)
  - [7. 실습 DAG 설명](#7-실습-dag-설명)
    - [응용 과제](#응용-과제)
  - [8. 트러블슈팅](#8-트러블슈팅)
    - [DAG가 UI 목록에 보이지 않을 때](#dag가-ui-목록에-보이지-않을-때)
    - [기타](#기타)
  - [9. 참고 자료](#9-참고-자료)

---

## 1. Airflow란

Apache Airflow는 워크플로우를 **Python 코드로 정의**하고, 정해진 스케줄에 따라 **자동 실행·모니터링**하는 오픈소스 오케스트레이션 플랫폼입니다. 2014년 Airbnb에서 시작해 Apache 재단 최상위 프로젝트가 되었습니다.

### 사용 목적

| 문제 | Airflow의 해결 |
|---|---|
| 작업 간 순서·의존성 관리가 어렵다 | DAG로 의존성을 명시적으로 선언 |
| cron은 실패 시 재시도·복구가 수작업이다 | 자동 재시도, 실패 Task만 재실행, 과거 기간 재처리(backfill) |
| 어디서 멈췄는지 알기 어렵다 | 웹 UI에서 상태·로그를 실시간 확인 |
| 파이프라인 변경 이력 관리가 안 된다 | 파이프라인이 Python 파일이므로 Git으로 버전 관리 |

### 활용 예시

- **ETL/ELT**: 외부 API·DB → 정제 → 데이터 웨어하우스 적재
- **ML 파이프라인**: 데이터 준비 → 학습 → 평가 → 조건부 배포
- **리포팅 자동화**: 집계 → 대시보드 갱신 → Slack/이메일 발송
- **운영 작업**: 주기적 백업, 로그 정리, 데이터 품질 검사
- **관리형 서비스**: Google Cloud Composer, AWS MWAA, Astronomer

---

## 2. 핵심 개념

| 개념 | 설명 |
|---|---|
| **DAG** | Directed Acyclic Graph. Task들의 실행 순서와 의존성을 정의한 파이프라인 단위 |
| **Task** | DAG를 구성하는 개별 작업 (예: extract, transform, load) |
| **Operator / `@task`** | Task를 만드는 템플릿. `BashOperator`, `PythonOperator` 등. Python 함수는 `@task` 데코레이터(TaskFlow API)로 바로 Task가 됨 |
| **DAG Run** | DAG의 1회 실행. 스케줄 또는 수동 트리거로 생성 |
| **Task Instance** | 특정 DAG Run 안에서의 Task 1회 실행. `queued / running / success / failed` 등 상태를 가짐 |
| **Schedule / logical_date** | 실행 주기(`@daily`, cron 표현식)와 해당 실행이 담당하는 데이터 기준 시각 |
| **XCom** | Task 간 작은 데이터를 주고받는 메커니즘. `@task` 함수의 반환값은 자동으로 XCom에 저장 |
| **Connection / Variable** | DB 접속 정보 등 비밀값·설정값을 코드 밖에서 관리 |
| **Sensor** | 파일 도착, 외부 작업 완료 등 조건 충족까지 대기하는 특수 Task |
| **Provider** | AWS, GCP, Slack 등 외부 시스템 연동 패키지 |

---

## 3. 아키텍처

```
┌──────────────┐                                   ┌───────────────────┐
│   DAG 파일    │                                   │  API 서버 · 웹 UI  │
│ (Python 코드) │                                   │ 모니터링 · 수동 실행 │
└──────┬───────┘                                   └─────────┬─────────┘
       │ 파싱                                                 │ 조회
       ▼                                                     │
┌──────────────┐     ┌──────────────┐     ┌──────────────┐    │
│   스케줄러    │ ──▶ │   Executor   │ ──▶ │    Worker    │    │
│  실행 시점 결정 │     │  실행 방식 결정 │     │ 태스크 실제 실행 │    │
└──────┬───────┘     └──────────────┘     └──────┬───────┘    │
       │                                        │            │
       ▼                                        ▼            ▼
┌──────────────────────────────────────────────────────────────────┐
│                         메타데이터 DB                             │
│                   DAG · Task 상태, 실행 이력 저장                   │
└──────────────────────────────────────────────────────────────────┘
```

| 컴포넌트 | 역할 |
|---|---|
| **DAG Processor** | `dags/` 폴더의 Python 파일을 주기적으로 파싱 (Airflow 3에서 별도 프로세스로 분리) |
| **Scheduler** | 스케줄에 따라 DAG Run을 만들고 Task를 Executor에 전달 |
| **Executor** | Task를 어떻게 실행할지 결정 (Local, Celery, Kubernetes 등) |
| **Worker** | Task를 실제로 실행 |
| **Triggerer** | 센서 등 대기 작업을 효율적으로 처리 |
| **API Server / Web UI** | 모니터링, 수동 실행, 로그 조회 |
| **Metadata DB** | 모든 상태·이력 저장 (실습에서는 SQLite) |

`airflow standalone` 명령은 위 컴포넌트를 한 번에 기동합니다.

---

## 4. 프로젝트 구조

```
.
├── README.md
├── .venv/                        # Python 가상환경 (Git 제외)
├── airflow.cfg                   # standalone 최초 실행 시 자동 생성
├── airflow.db                    # SQLite 메타데이터 DB (Git 제외)
├── simple_auth_manager_passwords.json.generated   # 로그인 비밀번호 (Git 제외)
├── logs/                         # Task 로그 (Git 제외)
└── dags/
    └── my_first_etl.py           # 실습 DAG
```

이 디렉토리 자체를 `AIRFLOW_HOME`으로 사용합니다.

권장 `.gitignore`:

```
.venv/
airflow.db
airflow.db-*
logs/
simple_auth_manager_passwords.json.generated
__pycache__/
```

---

## 5. 환경 구성

**요구 사항**: Python 3.10 ~ 3.12, Linux / macOS / WSL

```bash
# 1) 프로젝트 디렉토리로 이동 후 AIRFLOW_HOME 지정 (반드시 절대경로)
cd /path/to/this-project
export AIRFLOW_HOME="$(pwd)"

# 2) 가상환경
python -m venv .venv
source .venv/bin/activate

# 3) Airflow 설치 — constraint 파일과 함께 설치해야 의존성 충돌이 없음
AIRFLOW_VERSION=3.1.0
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# 4) 설치 확인
airflow version

# 5) dags 폴더 생성 (자동 생성되지 않음)
mkdir -p "$AIRFLOW_HOME/dags"
```

> **매번 export가 번거롭다면** `~/.bashrc` 또는 `~/.zshrc`에 다음을 추가하세요.
> ```bash
> export AIRFLOW_HOME=/path/to/this-project
> export AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL=30   # 새 DAG 파일 탐지 주기 단축 (개발용)
> ```

---

## 6. 실행 방법

```bash
# 반드시 AIRFLOW_HOME이 설정된 같은 터미널에서 실행
echo $AIRFLOW_HOME        # 프로젝트 경로가 나와야 함
airflow standalone
```

1. 브라우저에서 <http://localhost:8080> 접속
2. 계정: `admin` / 비밀번호는 터미널 출력 또는 `$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated` 파일 참고
3. DAGs 목록에서 `my_first_etl`을 찾아 **토글 ON**
4. ▶ 버튼으로 **Trigger** → Graph 뷰에서 Task가 초록색으로 바뀌는 과정 확인
5. Task 클릭 → **Logs** 탭에서 `print` 출력 확인

CLI로 테스트:

```bash
airflow dags list                          # 등록된 DAG 목록
airflow dags test my_first_etl 2026-10-01  # 스케줄러 없이 즉시 1회 실행
```

종료: 터미널에서 `Ctrl + C`

---

## 7. 실습 DAG 설명

`dags/my_first_etl.py`

```python
from datetime import datetime
from airflow.sdk import dag, task

@dag(
    dag_id="my_first_etl",
    schedule="@daily",            # 매일 1회
    start_date=datetime(2026, 10, 1),
    catchup=False,                # 과거 날짜 자동 재실행 안 함
    tags=["practice"],
)
def my_first_etl():

    @task
    def extract() -> list[int]:
        return [3, 7, 11, 20]       # 외부 API 호출 자리

    @task
    def transform(nums: list[int]) -> int:
        return sum(nums)

    @task
    def load(total: int):
        print(f"합계 {total} 저장 완료")   # DB 적재 자리

    @task
    def notify():
        print("파이프라인 성공!")

    load(transform(extract())) >> notify()

my_first_etl()
```

**흐름**: `extract → transform → load → notify`

- `extract`의 반환값이 XCom을 통해 `transform`의 인자로, 그 결과가 다시 `load`로 전달됩니다.
- `>>` 는 "앞 Task가 성공하면 다음 Task 실행"이라는 의존성입니다.
- `catchup=False`가 없으면 `start_date`부터 오늘까지의 모든 날짜에 대해 DAG Run이 생성됩니다.

### 응용 과제

1. `transform`에 `raise ValueError("테스트 실패")`를 넣어 실패시킨 뒤, UI에서 해당 Task만 **Clear**로 재실행해 보기
2. `@task(retries=2, retry_delay=timedelta(seconds=10))`로 자동 재시도 설정해 보기
3. `schedule="*/5 * * * *"` 로 바꿔 5분마다 실행되는지 확인하기
4. `BashOperator`로 `echo` 명령을 실행하는 Task를 추가해 보기

---

## 8. 트러블슈팅

### DAG가 UI 목록에 보이지 않을 때

점검 순서대로 확인합니다.

| # | 확인 | 명령 / 조치 |
|---|---|---|
| 1 | Airflow가 보는 dags 폴더가 맞는가 | `airflow config get-value core dags_folder` 출력이 실제 파일 위치와 같은지 확인 |
| 2 | `AIRFLOW_HOME`이 standalone을 띄운 셸에 설정됐는가 | `echo $AIRFLOW_HOME` 확인 후 standalone 재시작. export는 해당 터미널 세션에만 적용됨 |
| 3 | 경로에 `~`를 따옴표 안에 쓰지 않았는가 | `export AIRFLOW_HOME='~/project'`는 `~`가 치환되지 않음. 절대경로 사용 |
| 4 | `dags/` 폴더가 있고 파일이 그 안에 있는가 | `ls $AIRFLOW_HOME/dags` |
| 5 | 임포트 에러가 없는가 | `airflow dags list-import-errors`, 또는 UI 상단 Import errors 배너 확인 |
| 6 | 탐지 주기를 기다렸는가 | 새 파일 탐지 기본 주기 **5분**. 즉시 반영하려면 `airflow dags reserialize` 또는 standalone 재시작 |
| 7 | 파일 자체가 정상인가 | `python dags/my_first_etl.py` 가 에러 없이 끝나는지 확인 |

### 기타

| 증상 | 원인 / 조치 |
|---|---|
| `ImportError: cannot import name 'dag' from 'airflow.sdk'` | Airflow 2.x가 설치됨. `airflow version` 확인. 2.x에서는 `from airflow.decorators import dag, task` 사용 |
| 비밀번호가 터미널에 안 나옴 | Airflow 3.x 정상 동작. `simple_auth_manager_passwords.json.generated` 파일 확인 |
| 포트 8080 충돌 | `airflow.cfg`의 `[api] port` 값 변경 후 재시작 |
| pip 설치 중 의존성 충돌 | constraint 파일 없이 설치한 경우. 가상환경을 지우고 5장 절차대로 재설치 |

---

## 9. 참고 자료

- [Apache Airflow 공식 문서 – Architecture Overview](https://airflow.apache.org/docs/stable/core-concepts/overview.html)
- [Apache Airflow 공식 문서 – Quick Start](https://airflow.apache.org/docs/stable/start.html)
- [Apache Airflow 공식 문서 – Tutorials](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/index.html)
- [Apache Airflow 공식 문서 – Configuration Reference](https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html)
- [Wikipedia – Apache Airflow](https://en.wikipedia.org/wiki/Apache_Airflow)

---
