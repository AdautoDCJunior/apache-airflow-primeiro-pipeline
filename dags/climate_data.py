import os
from os.path import join
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.macros import ds_add
import pendulum
import pandas as pd

load_dotenv(find_dotenv())

with DAG(
  dag_id='climate_data',
  start_date=pendulum.datetime(2022, 11, 1, tz='UTC'),
  schedule_interval='0 0 * * 1'
) as dag:
  task_01 = BashOperator(
    task_id='cria_pasta',
    bash_command=
      'mkdir -p "/home/adauto_junior/cursos/alura/apache-airflow-primeiro-pipeline/temp/semana={{data_interval_end.strftime("%Y-%m-%d")}}"'
  )

  def data_extract(start_date):
    city = 'Boston'
    key = os.getenv('KEY')

    URL = join(
      'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
      f'{city}/{start_date}/{ds_add(start_date, 7)}?unitGroup=metric&include=days&key={key}&contentType=csv'
    )

    data = pd.read_csv(URL)

    file_path = join(Path(__file__).parents[1], 'temp', f'semana={start_date}')
    data.to_csv(join(file_path, 'dados_brutos.csv'))
    data[['datetime', 'tempmin', 'temp', 'tempmax']].to_csv(join(
      file_path,
      'temperaturas.csv'
    ))
    data[['datetime', 'description', 'icon']].to_csv(join(
      file_path,
      'condicoes.csv'
    ))

  task_02 = PythonOperator(
    task_id='extrai_dados',
    python_callable=data_extract,
    op_kwargs={'start_date': '{{data_interval_end.strftime("%Y-%m-%d")}}'}
  )

  task_01 >> task_02