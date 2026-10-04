"""
### 일별 매출 리포트 파이프라인 (학습용 예제)

DAG의 주요 기능을 한 파일에서 볼 수 있도록 구성한 예제입니다.

- **순차 실행**      : start → check_env
- **병렬 실행(fan-out)** : check_env → extract_sales, extract_users
- **합류(fan-in)**     : extract_sales + extract_users → merge
- **동적 태스크 매핑**  : summarize_region 이 지역 수만큼 자동 복제
- **XCom**            : @task 반환값이 다음 태스크의 인자로 전달
- **분기(branch)**     : 합계가 기준 이상이면 send_report, 아니면 skip_report
- **trigger_rule**    : 한쪽만 실행돼도 end 가 실행되도록 설정
- **재시도/기본 인자**  : default_args 로 모든 태스크에 retries 적용
- **Jinja 템플릿**     : BashOperator 에서 {{ ds }} 사용
- **params**          : UI 에서 Trigger 할 때 threshold 값을 바꿀 수 있음
"""

from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

REGIONS = ["seoul", "busan", "daegu"]

# 모든 태스크에 공통으로 적용되는 기본값
default_args = {
    "owner": "data-team",
    "retries": 2,                          # 실패 시 2번까지 재시도
    "retry_delay": timedelta(minutes=1),   # 재시도 간격
}


@dag(
    dag_id="daily_sales_report",
    schedule="0 2 * * *",                  # 매일 새벽 2시 (cron 표현식)
    start_date=datetime(2026, 10, 1),
    catchup=False,                         # 과거 날짜 자동 실행 안 함
    default_args=default_args,
    params={"threshold": 100},             # UI Trigger 시 수정 가능한 값
    tags=["practice", "example"],
    doc_md=__doc__,                        # 위 docstring 이 UI 의 DAG Docs 에 표시됨
)
def daily_sales_report():

    # ── 1. 전통적인 Operator 방식 ─────────────────────────────────────────
    start = EmptyOperator(task_id="start")   # 아무 일도 안 하는 표지판 역할

    check_env = BashOperator(
        task_id="check_env",
        # {{ ds }} 는 Jinja 템플릿. 실행 기준일(YYYY-MM-DD)로 치환된다.
        bash_command='echo "실행 기준일: {{ ds }}" && python --version',
    )

    # ── 2. TaskFlow(@task) 방식: 병렬 추출 ────────────────────────────────
    @task
    def extract_sales(ds=None) -> list[dict]:
        """ds 처럼 Airflow 컨텍스트 변수는 인자 이름만 맞추면 자동 주입된다."""
        print(f"{ds} 기준 매출 데이터 수집")
        return [  # 실제로는 API/DB 호출 자리
            {"region": "seoul", "amount": 120, "user_id": "1"},
            {"region": "seoul", "amount": 80,  "user_id": "2"},
            {"region": "busan", "amount": 60,  "user_id": "3"},
            {"region": "daegu", "amount": 30,  "user_id": "1"},
        ]

    @task
    def extract_users() -> dict[str, str]:
        # XCom 은 JSON 으로 저장되므로 dict 키는 문자열을 쓰는 것이 안전하다.
        raise ValueError("테스트용 에러 발생")  # 에러 발생 시 재시도됨
        return {"1": "Kim", "2": "Lee", "3": "Park"}

    # ── 3. fan-in: 두 결과를 합친다 ───────────────────────────────────────
    @task
    def merge(sales: list[dict], users: dict[str, str]) -> list[dict]:
        for row in sales:
            row["user_name"] = users.get(row["user_id"], "unknown")
        print(f"병합 완료: {len(sales)}건")
        return sales

    # ── 4. 동적 태스크 매핑: 지역 수만큼 태스크가 자동 생성된다 ─────────────
    @task
    def summarize_region(records: list[dict], region: str) -> dict:
        total = sum(r["amount"] for r in records if r["region"] == region)
        print(f"[{region}] 합계 = {total}")
        return {"region": region, "total": total}

    @task
    def aggregate(region_totals) -> int:
        # 매핑된 태스크의 결과는 lazy 시퀀스로 들어오므로 list() 로 풀어준다.
        totals = list(region_totals)
        grand_total = sum(t["total"] for t in totals)
        print(f"전체 합계 = {grand_total}, 지역별 = {totals}")
        return grand_total

    # ── 5. 분기: 다음에 실행할 task_id 를 문자열로 반환한다 ──────────────
    @task.branch
    def decide(total: int, params=None) -> str:
        threshold = params["threshold"]
        print(f"합계 {total} vs 기준 {threshold}")
        return "send_report" if total >= threshold else "skip_report"

    @task
    def send_report(total: int):
        print(f"📧 리포트 발송: 오늘 매출 합계는 {total} 입니다.")  # Slack/메일 자리

    skip_report = EmptyOperator(task_id="skip_report")

    # 분기로 한쪽은 skipped 가 되므로, 기본 규칙(all_success)이면 end 도 skipped 된다.
    # "실패가 없고, 하나 이상 성공했으면 실행" 규칙으로 바꿔준다.
    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    # ── 6. 의존성 연결 ───────────────────────────────────────────────────
    sales = extract_sales()
    users = extract_users()

    start >> check_env >> [sales, users]            # fan-out

    merged = merge(sales, users)                     # fan-in (XCom 전달)

    region_totals = summarize_region.partial(records=merged).expand(region=REGIONS)
    #                 partial(): 모든 복제본에 공통인 인자
    #                 expand() : 복제본마다 달라지는 인자 → 3개의 태스크 인스턴스 생성

    total = aggregate(region_totals)
    branch = decide(total)
    report = send_report(total)

    branch >> [report, skip_report] >> end


daily_sales_report()