from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    'owner':'teja003',
    'retries':5,
    'retry_delay':timedelta(minutes=1)
}

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
    import xml.etree.ElementTree as ET
    filename = ti.xcom_pull(task_ids="extract",key="raw_file")
    with open(filename,'r') as f:
        document = f.read()
    soup = BeautifulSoup(document,"xml")
    news_items = soup.find_all("item")
    xml_data = ET.Element("list")
    for i in news_items:
        data = ET.SubElement(xml_data,'item')
        title_ele = ET.SubElement(data, 'title')
        title_ele.text = i.find("title").text
        desc_ele = ET.SubElement(data, 'description')
        desc_ele.text = i.find("description").text
        link_ele = ET.SubElement(data, 'link')
        link_ele.text = i.find("link").text
        guid_ele = ET.SubElement(data, 'guid')
        guid_ele.text = i.find("guid").text
        pubDate_ele = ET.SubElement(data, 'pubDate')
        pubDate_ele.text = i.find("pubDate").text
    write_data = ET.tostring(xml_data)
    today = datetime.now()
    d1 = today.strftime("%Y%m%d%H%M%S")
    file = f"curated_{d1}.xml"
    ti.xcom_push(key="curated_file",value=file)
    with open(file, "wb") as f:
        f.write(write_data)

def loadfn(ti):
    from bs4 import BeautifulSoup
    import sqlite3
    filename = ti.xcom_pull(task_ids="transform",key="curated_file")
    with open(filename,'r') as f:
        document = f.read()
    soup = BeautifulSoup(document,"xml")
    titles = soup.find_all("title")
    descs = soup.find_all('description')
    pubDates = soup.find_all('pubDate')
    conn = sqlite3.connect('rss_feed.db')
    cursor = conn.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS FEED(
    Title VARCHAR(255) NOT NULL,
    Description VARCHAR(255) NOT NULL,
    PubDate VARCHAR(50)
    )""")
    for i in zip(titles,descs,pubDates):
        title = i[0].text.split('"')
        title = "'".join(title)

        desc = i[1].text.split('"')
        desc = "'".join(desc)

        pubDate = i[2].text.split('"')
        pubDate = "'".join(pubDate)

        table = f"INSERT INTO FEED VALUES(\"{title}\",\"{desc}\",\"{pubDate}\")"
        print(table)
        cursor.execute(table)
    conn.commit()
    conn.close()
    print("Done")


with DAG(
    dag_id = "rss_feed_etl_xml",
    default_args=default_args,
    description = "rss feed etl",
    start_date = datetime(2023,7,19),
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