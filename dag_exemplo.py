from datetime import datetime, timedetal
from airflow import DAG
from operators.databricks import DatabricksPythonOperator
from operatos.databricks import DatabricksNotebookOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# CONFIG
pipeline_name = "Purview_Extract"
owner = "gabriel-augusto"
email_owner = "gabriel.haugusto@xpi.com.br"
team = "data-governance"
company = "xp"
tags = [pipeline_name, team, company]

DEFAULT_ARGS = {
    "max_active_runs": 1,
    "owner": owner,
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 6, 1)
}

with DAG(
    dag_id=pipeline_name,
    default_args=DEFAULT_ARGS,
    schedule_interval="0 0 * * *",
    catchup=False,
    tags=tags
) as dag:
    
    bronze = DatabricksPythonOperator(
        task_id="extract_purview_data",
        python_file="/Shared/bronze.py",
        databricks_conn_id="databricks_default"
        cluster_config={
            "new_cluster": {
                "spark_version": "12.0.x-scala-2.12",
                "node_type_id": "Standard_DS3_v2",
                "num_workers": 2
            }
        }
    )
    
    trigger_next_dag = TriggerDagRunOperator(
        task_id="trigger_next_dag",
        trigger_dag_id="Next_DAG_Name",
        wait_for_completion=True
    )
    
    bronze >> trigger_next_dag