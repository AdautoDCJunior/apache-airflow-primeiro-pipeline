from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

def greeted():
  print('Boas-vindas ao Airflow')

with DAG (
  dag_id='task_class_04',
  start_date=days_ago(1),
  schedule='@daily'
) as dag:
  task_1 = PythonOperator(
    task_id='greeted_task',
    python_callable=greeted
  )
