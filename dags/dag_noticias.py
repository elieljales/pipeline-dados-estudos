import json
from datetime import datetime, timedelta

from airflow.models import DAG, TaskInstance
from airflow.operators.python_operator import PythonOperator
from operators.g1_operator import G1Operator

def insert_data(**context):
    # pass
    list_test = context.get("ti").xcom_pull(key='news')
    print(list_test[0][0])
    es = Elasticsearch(host="es01", port=9200)
    for data in list_test[0]:
        res = es.index(index='news-index', doc_type='authors',id=data['id'], body=data)

with DAG(
    dag_id="dag_noticias",
    schedule_interval="@daily",
    default_args={
        "owner":"airflow",
        "retries":1,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime(2021,10,1)
    },
    catchup=False) as f:
        get_news_links = G1Operator(
            task_id= "g1_crawler",
            pages = 4,
        ),
        
        
        insert_data = PythonOperator(
            task_id= "insert_data",
            python_callable=insert_data,
            params={"date": datetime.now().strftime('%Y%m%d')},
            provide_context=True,
            op_kwargs={"name":"Eliel Jales"}
        )
        
get_news_links
