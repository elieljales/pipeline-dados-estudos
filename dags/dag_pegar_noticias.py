
try:
    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    import requests
    import xmltodict
    from lxml import html
    from bs4 import BeautifulSoup
    import json
    import hashlib
    from elasticsearch import Elasticsearch
    print("All Dag modules are ok.....")

except Exception as e:
    print("Error {}".format(e))


def get_news(url,**context):
    page = requests.get(url)
    content = html.fromstring(page.content)
    soup = BeautifulSoup(page.content, 'html5lib')
    
    news_title = content.xpath('/html/body/div[1]/main/div[2]/div[1]/h1/text()')
    sub_title = content.xpath("/html/body/div[1]/main/div[2]/div[2]/h2/text()")
    try:
        news_article = soup.find_all('article')[0].get_text()
    except:
        news_article = ''
    published_date = content.xpath("/html/body/div[1]/main/div[3]/div[1]/div/div/p[2]/time/text()")
    try:
        published_date = published_date[0].split(' ')[1]
    except Exception as e:
        print(e)
    datasource = 'G1'
    news = [ treatment_string(news_title),  treatment_string(sub_title), treatment_string(news_article), published_date , url, datasource] 
    for i in range(len(news)):
        news[i] = str(news[i]).strip("[]")
        news[i] = treatment_string(news[i])
    id_news = hashlib.md5(news[0].encode('utf-8')).hexdigest()
    news_json = {
        'id': id_news,
        "titulo" : news[0],
        "resumo" : news[1],
        "conteudo" : news[2],
        "data_publicacao" : news[3],
        "url_fonte": news[4],
        "fonte": news[5]
    }
    return news_json

def treatment_string(string):
    s = ''.join(string)
    string = s
    string = string.replace('\n', ' ')
    string = string.replace('\r', ' ')
    string = string.replace('1 de 3', '')
    string = string.replace('2 de 3', '')
    string = string.replace('3 de 3', '')
    string = string.replace("'", "")
    
    ready = False
    while ready == False:
        pos = string.find("  ")
        if pos != -1:
            string = string.replace("  "," ")
        else:
            ready = True
            
            
    return string
    

def get_news_links(params,**context):

    for i in range(1,3):
        page = requests.get(f"https://g1.globo.com/index/feed/pagina-{i}.ghtml")
        content = html.fromstring(page.content)
        lastest_news = []
        print("Extraindo Pagina "+ str(i))  
        
        for item in range(1,11):
            notice = content.xpath(f"//*[@id='feed-placeholder']/div/div/div[2]/div/div/div/div[{item}]/div/div/div/div[2]/div/div/a/@href")
            try:
                if 'https://g1.globo.com/globonews' in notice[0]:
                    pass
                
                elif 'https://g1.globo.com' in notice[0]:
                    lastest_news.append(get_news(notice[0]))
            except Exception as e:
                print("Error no primeiro try " + str(e))
                break
    
    try:    
        instance = context.get("ti").xcom_pull(key='news')
        print(type(instance))
        instance.append(lastest_news)
        context['ti'].xcom_push(key='news', value=instance)
    except Exception as e:
        context['ti'].xcom_push(key='news', value=[lastest_news])
        print("Error no segundo try " + str(e))

def insert_data(**context):
    # pass
    list_test = context.get("ti").xcom_pull(key='news')
    print(list_test[0][0])
    es = Elasticsearch(host="es01", port=9200)
    for data in list_test[0]:
        res = es.index(index='news-index', doc_type='authors',id=data['id'], body=data)
     
        
with DAG(
    dag_id="dag_pegar_noticias",
    schedule_interval="@daily",
    default_args={
        "owner":"airflow",
        "retries":1,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime(2021,8,31)
    },
    catchup=False) as f:
        get_news_links = PythonOperator(
            task_id= "get_news_links",
            python_callable=get_news_links,
            params={"date": datetime.now().strftime('%Y%m%d')},
            provide_context=True,
            op_kwargs={"name":"Eliel Jales"}
        ),
        
        insert_data = PythonOperator(
            task_id= "insert_data",
            python_callable=insert_data,
            params={"date": datetime.now().strftime('%Y%m%d')},
            provide_context=True,
            op_kwargs={"name":"Eliel Jales"}
        )
        
get_news_links >> insert_data