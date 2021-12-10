from datetime import datetime
from tempfile import NamedTemporaryFile

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import DAG
from airflow.sensors.http_sensor import HttpSensor
from airflow.hooks.http_hook import HttpHook
from airflow.operators.python_operator import PythonOperator

BASE_URI = "CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/"
URI_TEMPLATE = BASE_URI + "{{ execution_date.strftime('%m-%d-%Y') }}.csv"

dag = DAG(dag_id='jhu_csse_covid_processing',
          max_active_runs=1,
          start_date=datetime(2020, 1, 22),
          schedule_interval='@daily')

# Wait for the file to be published
http_file_sensor = HttpSensor(
    task_id='wait_for_data_file',
    http_conn_id='http_jhu_covid_github',
    endpoint=URI_TEMPLATE,
    poke_interval=3600,
    timeout=86400,
    method='HEAD',
    dag=dag
)


def create_table_for_csv(csv_file, table_name):
    header = csv_file.readline()
    field_names = header.split(',')
    column_specs = ','.join(map(lambda field_name: '"' + field_name.strip() + '"' + " TEXT", field_names))
    return """CREATE TABLE {table_name} ( {column_specs} );""".format(table_name=table_name, column_specs=column_specs)


def load_jhu_file_to_database(**context):
    http_hook = HttpHook(http_conn_id='http_jhu_covid_github',
                         method='GET')
    postgres_hook = PostgresHook(postgres_conn_id='covid-database')
    response = http_hook.run(BASE_URI + context['execution_date'].strftime('%m-%d-%Y') + '.csv')

    with NamedTemporaryFile(mode='w', encoding='utf-8') as temp_file:
        temp_file.write(response.text.lstrip('\ufeff'))
        temp_file.flush()

        table_name = 'jhu.jhu_data_' + context['execution_date'].strftime('%Y%m%d')

        postgres_hook.run(create_table_for_csv(open(temp_file.name, 'r'), table_name), autocommit=True)
        postgres_hook.copy_expert(sql='COPY {table_name} FROM STDIN CSV HEADER'.format(table_name=table_name),
                                  filename=temp_file.name)


data_load_operator = PythonOperator(task_id='load_data_file_into_database',
                                    python_callable=load_jhu_file_to_database,
                                    provide_context=True,
                                    dag=dag)

http_file_sensor >> data_load_operator
