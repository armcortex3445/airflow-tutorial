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