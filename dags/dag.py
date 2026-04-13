#DAG AIRFLOW ORIGINAL

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from tasks.bronze_ingest import (
    run_comments_pipeline,
    run_posts_pipeline,
    run_users_pipeline,
)

from tasks.silver_transform import (
    run_silver_users,
    run_silver_comments,
    run_silver_posts,
)
from tasks.gold_agg import run as run_gold


default_args = {
    "owner": "airflow",
}

with DAG(
    dag_id="proyecto2",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    comments_task = PythonOperator(
        task_id="comments_pipeline",
        python_callable=run_comments_pipeline,
    )

    posts_task = PythonOperator(
        task_id="posts_pipeline",
        python_callable=run_posts_pipeline,
    )

    users_task = PythonOperator(
        task_id="users_pipeline",
        python_callable=run_users_pipeline,
    )

    silver_users_task = PythonOperator(
        task_id="silver_users_pipeline",
        python_callable=run_silver_users,
    )

    silver_comments_task = PythonOperator(
        task_id="silver_comments_pipeline",
        python_callable=run_silver_comments,
    )

    silver_posts_task = PythonOperator(
        task_id="silver_posts_pipeline",
        python_callable=run_silver_posts,
    )

    gold_task = PythonOperator(
        task_id="gold_cant_post_x_user_hist",
        python_callable=run_gold,
    )


     # Toda la Bronze debe completar antes de arrancar Silver
     
    users_task >> silver_users_task
    comments_task >> silver_comments_task
    posts_task >> silver_posts_task

    # Toda la Silver debe completar antes de arrancar Gold
    [silver_users_task, silver_comments_task, silver_posts_task] >> gold_task
