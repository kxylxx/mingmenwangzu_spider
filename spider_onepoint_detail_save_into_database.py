from selenium import webdriver
from pyquery import PyQuery as pq
import time
from bs4 import BeautifulSoup
import pymysql
import pandas as pd
import numpy as np
from config_db import *

def Get_newsurl(table_name):
    config_dbname = {
        'host': MYSQL_URL,
        'user': MYSQL_USER,
        'password': MYSQL_PASSWORD,
        'db' : MYSQL_DB,
        'port': MYSQL_PORT,
        'charset': MYSQL_CHARSET
    }
    conn = pymysql.connect(**config_dbname)  # 连接数据库
    sql_search = "select article_url from %s " % table_name
    df = pd.read_sql(sql_search, con=conn)
    conn.close()
    url_list = np.array(df)
    return url_list


def Get_news_detail(url_list):
    #print(url_list[1][0])
    brower = webdriver.Chrome()
    articles = []
    print(len(url_list))
    for i in range(len(url_list)):
        url = url_list[i][0]
        brower.get(url)
        html = brower.page_source
        doc = pq(html)
        soup = BeautifulSoup(html, "html.parser")

        try:
            brower.execute_script('window.scrollTo(0, document.body.scrollHeight)')
            time.sleep(1)
            for article in soup.find_all(class_='left-wrapper'):
                news_title = article.find('h2').get_text()
                #一点咨询有的writer时候其他网站来源也存在span标签下，导致抓取出错，所以不提取作者信息和时间信息了，最后更新之前的列表就好
                #news_writer = article.find(class_='doc-source').get_text()
                #news_date = article.find('span').get_text()
                #news_content = doc('#imedia-article').children().text()#有些文章没有该标签
                news_content = article.find(class_='content-bd').get_text()
                news_url = url
                articles.append([news_title, news_content,news_url])
        except Exception as e:
            # print(e)
            pass
    print(len(articles))
    brower.close()
    return articles

def insert_into_table(table_name,articles,url_list):
    config_dbname = {
        'host': MYSQL_URL,
        'user': MYSQL_USER,
        'password': MYSQL_PASSWORD,
        'db': MYSQL_DB,
        'port': MYSQL_PORT,
        'charset': MYSQL_CHARSET
    }
    conn = pymysql.connect(**config_dbname)  # 连接数据库
    cursor = conn.cursor()
    try:
        sql_alter = "alter table %s add (article_content varchar(65533))" %table_name
        cursor.execute(sql_alter)
        try:
            for i in range(len(articles)):
                for j in range(len(url_list)):
                    if articles[i][2] == url_list[j][0]:
                        # print(articles[i][2])
                        sql_update = "update %s set article_content='%s' where article_url='%s'" % (table_name, articles[i][1], articles[i][2])
                        cursor.execute(sql_update)
                        conn.commit()
                    print("更新成功")
        except Exception as e1:
            print(e1)
    except Exception as e:
        print(e)
        conn.rollback()
    conn.close()
    cursor.close()

def main():
    for keyword,table_name in table_name_list.items():
        url_list=Get_newsurl(table_name)
        articles = Get_news_detail(url_list)
        insert_into_table(table_name, articles, url_list)
        #print(url_list)
main()



