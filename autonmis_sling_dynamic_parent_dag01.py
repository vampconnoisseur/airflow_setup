import os
from airflow import DAG
from airflow.models import Variable
import pytz
from datetime import datetime
import json

IST = pytz.timezone('Asia/Kolkata')

def create_setup_dag_code(child_dag_var, schedule, email, env):
    dag_code = f"""
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
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
dag_id = config.get('dag_id')+"_setup"
sling_api_base_url = "http://airflow-flask-app-1:5001"

dag = DAG(
    dag_id,
    default_args=default_args,
    description='Setup DAG for Autonomis with Sling integration',
    schedule_interval='{schedule}',
    catchup=False,
    tags=["setup-sling-dag"],
)

def call_sling_api(endpoint, payload):
    url = f"{{sling_api_base_url}}{{endpoint}}"
    headers = {{"Content-Type": "application/json"}}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()

def set_connection(**kwargs):
    conn_config = kwargs['conn_config']
    if conn_config['conn'] is not None:  # Only set connection if it's not a file-based source
        payload = {{
            "conn": conn_config['conn'],
            "details": conn_config['details']
        }}
        return call_sling_api("/set_connection", payload)

def register_job(**kwargs):
    payload = {{
        "dag_id": config['dag_id'],
        "source": {{
            "conn": config['source'].get('conn'),
            "stream": config['source']['stream']
        }},
        "target": {{
            "conn": config['target']['conn'],
            "object": config['target']['object']
        }},
        "mode": config.get('mode', 'full-refresh')
    }}
    if 'source_options' in config:
        payload['source_options'] = config['source_options']
    if 'target_options' in config:
        payload['target_options'] = config['target_options']
    if 'primary_key' in config:
        payload['primary_key'] = config['primary_key']
    if 'update_key' in config:
        payload['update_key'] = config['update_key']
    
    return call_sling_api("/register", payload)

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

    [set_source_connection, set_target_connection] >> register_sling_job

"""
    return dag_code

def create_run_job_dag_code(child_dag_var, schedule, email, env):
    dag_code = f"""
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
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
#dag_id = f"{config.get('dag_id')}_run"
dag_id = config.get('dag_id')+"_run"
sling_api_base_url = "http://airflow-flask-app-1:5001"

dag = DAG(
    dag_id,
    default_args=default_args,
    description='Run Job DAG for Autonomis with Sling integration',
    schedule_interval='{schedule}',
    catchup=False,
    tags=["run-sling-dag"],
)

def call_sling_api(endpoint, payload):
    url = f"{{sling_api_base_url}}{{endpoint}}"
    headers = {{"Content-Type": "application/json"}}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()

def run_job(**kwargs):
    payload = {{
        "dag_id": config['dag_id']
    }}
    return call_sling_api("/run", payload)

with dag:
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

    run_sling_job >> email_task

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
    
    # Create Setup DAG
    setup_dag_code = create_setup_dag_code(child_dag_var, child_dag_schedule, default_email, env)
    setup_file_path = os.path.join(dags_folder, f"{child_dag_var}_setup.py")
    with open(setup_file_path, 'w') as file:
        file.write(setup_dag_code)

    # Create Run Job DAG
    run_job_dag_code = create_run_job_dag_code(child_dag_var, child_dag_schedule, default_email, env)
    run_job_file_path = os.path.join(dags_folder, f"{child_dag_var}_run.py")
    with open(run_job_file_path, 'w') as file:
        file.write(run_job_dag_code)
