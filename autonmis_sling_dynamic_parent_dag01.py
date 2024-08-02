import os
from airflow import DAG
from airflow.models import Variable
import pytz
from datetime import datetime
import json

IST = pytz.timezone('Asia/Kolkata')

def create_dag_code(child_dag_var, schedule, email, env):
    dag_code = f"""
import os
from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import json
import requests

default_args = {{
    'owner': 'airflow',
    'email_on_failure': True,
    'email': ['{email}'],
    'email_on_retry': False,
    'depends_on_past': False,
    'start_date': days_ago(1),
}}

dynamic_var_name = "{child_dag_var}"
config = Variable.get(dynamic_var_name, deserialize_json=True)
schedule_interval = config.get('schedule_interval', '*/55 * * * *')
dag_id = config.get('dag_id')
sling_api_base_url = "http://airflow-flask-app-1:5001"

dag = DAG(
    dag_id,
    default_args=default_args,
    description='A dynamic DAG for Autonomis with Sling integration',
    schedule_interval=schedule_interval,
    catchup=False,
    tags=["dynamic-sling-dag"],
)

def call_sling_api(endpoint, payload):
    url = f"{{sling_api_base_url}}{{endpoint}}"
    headers = {{"Content-Type": "application/json"}}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()

def set_connection(**kwargs):
    conn_config = kwargs['conn_config']
    payload = {{
        "name": conn_config['conn'],
        "details": conn_config['details']
    }}
    return call_sling_api("/set_connection", payload)

def register_job(**kwargs):
    payload = {{
        "source": {{
            "conn": config['source']['conn'],
            "stream": config['source']['stream']
        }},
        "target": {{
            "conn": config['target']['conn'],
            "object": config['target']['object']
        }},
        "mode": config.get('mode', 'full-refresh')
    }}
    return call_sling_api("/register", payload)

def run_job(**kwargs):
    payload = {{"stream": config['source']['stream']}}
    return call_sling_api("/run", payload)

with dag:
    set_source_connection = PythonOperator(
        task_id='set_source_connection',
        python_callable=set_connection,
        op_kwargs={{'conn_config': config['source']}},
        dag=dag
    )

    set_target_connection = PythonOperator(
        task_id='set_target_connection',
        python_callable=set_connection,
        op_kwargs={{'conn_config': config['target']}},
        dag=dag
    )

    register_sling_job = PythonOperator(
        task_id='register_sling_job',
        python_callable=register_job,
        dag=dag
    )

    run_sling_job = PythonOperator(
        task_id='run_sling_job',
        python_callable=run_job,
        dag=dag
    )

    email_task = EmailOperator(
        task_id='send_email',
        to=["{email}"],
        subject=f"Sling Job Completion: {{{{dag.dag_id}}}}",
        html_content="Sling job execution completed.",
        dag=dag
    )

    [set_source_connection, set_target_connection] >> register_sling_job >> run_sling_job >> email_task

"""
    return dag_code

# Retrieve the configuration for the dynamic DAGs from Airflow Variables
dag_configs = Variable.get("parent_sling_dag_var", deserialize_json=True)
dags_folder = f'/opt/airflow/dags/'
default_email = 'abhranshu.bagchi@gmail.com'
env = os.getenv('ENV', 'development')

# Generate and write the DAG code for each child DAG configuration
for config in dag_configs['dags']:
    child_dag_var = config['child_dag_var']
    child_dag_schedule = config['child_dag_schedule']
    dag_code = create_dag_code(child_dag_var, child_dag_schedule, default_email, env)
    file_path = os.path.join(dags_folder, f"{child_dag_var}.py")
    with open(file_path, 'w') as file:
        file.write(dag_code)