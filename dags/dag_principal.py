from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from cotizaciones_api import conexion_redshift, insercion_datos, get_cotizaciones

default_args={
    'owner' : 'Bautista Argañaraz Londero',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=3)
}

dag_principal = DAG(
    default_args = default_args,
    dag_id = 'cotizaciones_7_magnificos',
    description = 'Obtiene las cotizaciones de los 7 magníficos de NASDAQ y los carga en la tabla en AWS Redshift con una frecuencia diaria.',
    start_date = datetime(2024,3,27,2),
    schedule_interval = '@daily',
    catchup = False
)

def task1_get_cotizaciones(**kwargs):
    return get_cotizaciones()

def task3_insercion_datos(**kwargs):
    ti = kwargs['ti']
    cotizaciones_json = ti.xcom_pull(task_ids='get_cotizaciones')
    insercion_datos(cotizaciones_json)

task1 = PythonOperator(
    task_id = 'get_cotizaciones',
    python_callable = task1_get_cotizaciones,
    provide_context=True,
    dag = dag_principal,
)

task2 = PythonOperator(
    task_id = 'conexion_redshift',
    python_callable = conexion_redshift,
    dag = dag_principal,
)

task3 = PythonOperator(
    task_id = 'insercion_datos',
    python_callable = task3_insercion_datos,
    provide_context=True,
    dag = dag_principal,
)

task1 >> task2 >> task3