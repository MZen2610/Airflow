from datetime import timedelta
import pandas as pd
import sqlite3

CON = sqlite3.connect('example.db')

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


def extract_data(url, tmp_file, **context) -> pd.DataFrame:
    pd.read_csv(url).to_csv(tmp_file)


def union_csv_file(tmp_file_1, tmp_file_2, table_name, conn=CON, **context) -> pd.DataFrame:
    df1 = pd.read_csv(tmp_file_1)
    df2 = pd.read_csv(tmp_file_2)
    frames = [df1, df2]
    result = pd.concat(frames)
    result.to_sql(table_name, conn, if_exists='replace', index=False)


default_args = {'owner': 'airflow',
                'retries': 5,
                'retry_delay': timedelta(minutes=5),
                'trigger_rule': 'all_success'
                }

with DAG(dag_id='dag',
         default_args=default_args,
         start_date=days_ago(1),
         schedule_interval='@daily',
         ) as dag:
    read_csv_file_1 = PythonOperator(
        task_id='read_csv_file_1',
        python_callable=extract_data,
        dag=dag,
        op_kwargs={
            'url': 'https://raw.githubusercontent.com/dm-novikov/stepik_airflow_course/main/data/project_1/data_1.csv',
            'tmp_file': '/tmp/file_1.csv'},
    )

    url_2 = 'https://raw.githubusercontent.com/dm-novikov/stepik_airflow_course/main/data/project_1/data_2.csv'
    read_csv_file_2 = BashOperator(
        task_id='read_csv_file_2',
        dag=dag,
        bash_command=f'wget -O /tmp/file_2.csv {url_2}',
    )

    union_csv_file = PythonOperator(
        task_id='union_csv_file',
        python_callable=union_csv_file,
        dag=dag,
        op_kwargs={
            'tmp_file_1': '/tmp/file_1.csv',
            'tmp_file_2': '/tmp/file_2.csv',
            'table_name': 'table',
        }
    )

    read_csv_file_1 >> read_csv_file_2 >> union_csv_file
