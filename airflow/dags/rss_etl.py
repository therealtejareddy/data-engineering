from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import csv

default_args = {
    'owner':'teja003',
    'retries':5,
    'retry_delay':timedelta(minutes=1)
}
#  sensory, branch
def extractfn(ti):
    import requests
    from bs4 import BeautifulSoup
    url = "https://timesofindia.indiatimes.com/rssfeedstopstories.cms"
    document = requests.get(url)
    soup = str(BeautifulSoup(document.content,"lxml-xml"))
    today = datetime.now()
    d1 = today.strftime("%Y%m%d%H%M%S")
    file = f"raw_rss_feed_{d1}.xml"
    ti.xcom_push(key="raw_file",value=file)
    with open(file, "w") as f:
        f.write(soup)

def transformfn(ti):
    from bs4 import BeautifulSoup
    filename = ti.xcom_pull(task_ids="extract",key="raw_file")
    with open(filename,'r') as f:
        document = f.read()
    soup = BeautifulSoup(document,"xml")
    news_items = soup.find_all("item")
    items = []
    for item in news_items:
        title = item.find('title').text
        link = item.find('link').text
        pub_date = item.find('pubDate').text
        items.append((title, link, pub_date))
    today = datetime.now()
    d1 = today.strftime("%Y%m%d%H%M%S")
    file = f"curated_{d1}.csv"
    ti.xcom_push(key="curated_file",value=file)
    with open(file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Title', 'Link', 'Pub Date'])
        writer.writerows(items)

def loadfn(ti):
    import pandas as pd
    import sqlite3
    conn = sqlite3.connect('rss_feed.db')
    cursor = conn.cursor()
    filename = ti.xcom_pull(task_ids="transform",key="curated_file")
    data = pd.read_csv(filename)
    data.to_sql('FEED', conn, if_exists='append', index=False)
    conn.commit()
    conn.close()
    print("Done")

with DAG(
    dag_id = "rss_etl",
    default_args=default_args,
    description = "rss feed etl",
    start_date = datetime(2023,7,23),
    schedule_interval='0 23 * * *',
) as dag:
    extract = PythonOperator(
        task_id="extract",
        python_callable=extractfn
    )
    transform = PythonOperator(
        task_id="transform",
        python_callable=transformfn
    )
    load=PythonOperator(
        task_id="load",
        python_callable=loadfn
    )
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    start >> extract >> transform >> load >> end