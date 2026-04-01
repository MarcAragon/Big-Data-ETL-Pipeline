from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from tasks.bronze_ingest import (
    run_comments_pipeline,
    run_posts_pipeline,
    run_users_pipeline
)


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

    [comments_task, posts_task, users_task]