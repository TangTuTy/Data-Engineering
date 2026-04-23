from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

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

default_args = {
    "owner": "mara",
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