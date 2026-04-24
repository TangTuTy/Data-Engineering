from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from bronze.bronze_layer import load_sp500_daily_to_bronze
from silver.silver_layer import (
    transform_historical_to_silver,
    classify_war_impact,
    enrich_company_profiles,
    transform_live_trades_to_silver,
)
from gold.gold_layer import (
    build_gold_sector_summary,
    build_gold_stock_ranking,
    build_gold_weekly_sector_performance,
    build_gold_war_daily_timeline,
)
from utils.alert_notifier import send_critical_pipeline_alert


def task_failure_alert(context):
    """
    Callback ที่ถูกเรียกเมื่อ task fail
    ส่ง alert เข้า Discord (ถ้าตั้ง DISCORD_WEBHOOK_URL ไว้)
    """
    task_instance = context.get('task_instance')
    dag_id   = context.get('dag').dag_id
    task_id  = task_instance.task_id
    log_url  = task_instance.log_url
    try_num  = task_instance.try_number
    exception = context.get('exception')

    message = (
        f"**DAG:** {dag_id}\n"
        f"**Task:** {task_id}\n"
        f"**Attempt:** {try_num}\n"
        f"**Error:** ```{str(exception)[:500]}```\n"
        f"**Log:** {log_url}"
    )

    send_critical_pipeline_alert(message=message, stage=f"{dag_id}.{task_id}")


# ───── Default args: retry + alert ─────
default_args = {
    "owner": "mara",
    "retries": 2,                          # retry 2 ครั้งถ้า fail
    "retry_delay": timedelta(minutes=2),   # รอ 2 นาทีก่อน retry
    "on_failure_callback": task_failure_alert,  # ส่ง Discord เมื่อ retry หมดแล้วยัง fail
}


with DAG(
    dag_id="sp500_daily_analytics_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule="0 18 * * 1-5",
    catchup=False,
    tags=["sp500", "daily", "analytics"],
) as dag:

    bronze_task = PythonOperator(
        task_id="load_sp500_daily_to_bronze",
        python_callable=load_sp500_daily_to_bronze,
    )

    silver_transform_task = PythonOperator(
        task_id="transform_historical_to_silver",
        python_callable=transform_historical_to_silver,
    )

    silver_classify_task = PythonOperator(
        task_id="classify_war_impact",
        python_callable=classify_war_impact,
    )

    silver_enrich_task = PythonOperator(
        task_id="enrich_company_profiles",
        python_callable=enrich_company_profiles,
    )

    silver_live_task = PythonOperator(
        task_id="transform_live_trades_to_silver",
        python_callable=transform_live_trades_to_silver,
    )

    gold_sector_task = PythonOperator(
        task_id="build_gold_sector_summary",
        python_callable=build_gold_sector_summary,
    )

    gold_ranking_task = PythonOperator(
        task_id="build_gold_stock_ranking",
        python_callable=build_gold_stock_ranking,
    )

    gold_weekly_task = PythonOperator(
        task_id="build_gold_weekly_sector_performance",
        python_callable=build_gold_weekly_sector_performance,
    )

    gold_timeline_task = PythonOperator(
        task_id="build_gold_war_daily_timeline",
        python_callable=build_gold_war_daily_timeline,
    )

    bronze_task >> silver_transform_task >> silver_classify_task
    silver_classify_task >> [silver_enrich_task, silver_live_task]

    silver_enrich_task >> gold_sector_task
    silver_enrich_task >> gold_ranking_task
    [silver_transform_task, silver_enrich_task] >> gold_weekly_task
    silver_transform_task >> gold_timeline_task