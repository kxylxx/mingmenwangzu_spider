import re
from urllib.parse import urlencode

import requests
import pandas as pd
import numpy as np
import pymysql
from bs4 import BeautifulSoup
from pyquery import PyQuery as pq
from config import *



def get_baidu_page_index(keyword,pn):
    data = {
        'word': keyword,
        'pn': pn,
        'tn': 'news'
    }
    params = urlencode(data)
    url = 'http://news.baidu.com/ns?' + params
    doc = pq(url)
    news_lists = doc('#wrapper_wrapper #content_left div.result').items()
    if news_lists:
        for news in news_lists:
            yield {
                'article_url': news.find('.c-title a').attr('href'),
                'article_title': news.find('.c-title a').text(),
                'article_source': re.search(re.compile('(.*?)\\xa0', re.S), news.find('p.c-author').text()).group(1)
            }


def get_sina_page_index(keyword,page):
    data = {
        'q': keyword,
        'c': 'news',
        'page': page
    }
    params = urlencode(data)
    base = 'http://search.sina.com.cn/?'
    url = base + params
    # print(url)
    doc = pq(url)
    news_lists = doc('div#result.result .box-result.clearfix').items()
    if news_lists:
        for news in news_lists:
            yield {
                'article_url': news.find('a').attr('href'),
                'article_title': news.find('a').text(),
                # 'article_source': news.find('span.fgray_time').text()
                'article_source': re.search(re.compile('([\u4e00-\u9fa5]*?) \d', re.S),
                                            news.find('span.fgray_time').text()).group(1)
            }


def get_sougo_page_index(keyword,page):
    data = {
        'query': keyword,
        'page': page
    }
    params = urlencode(data)
    base = 'http://news.sogou.com/news?'
    url = base + params
    # print(url)
    doc = pq(url)
    doc('div .wrapper .main .results .vrwrap:last-child').remove()
    news_lists = doc('div .wrapper .main .results .vrwrap').items()
    if news_lists:
        for news in news_lists:
            yield {
                'article_url': news.find('.vrTitle a').attr('href'),
                'article_title': news.find('.vrTitle a').text(),
                # 'article_source': news.find('.news-detail .news-from').text()
                'article_source': re.search(re.compile('([\u4e00-\u9fa5]*?)\\xa0', re.S),
                                            news.find('.news-detail .news-from').text()).group(1)
            }


def articles_pool(articles):
    for article in articles:
        articles_pool_list.append(article)


def save_to_mysql(table_name,articles_pool):
    # 连接数据库，新建一个数据库
    config = {
        'host' : MYSQL_URL,
        'user' : MYSQL_USER,
        'password' : MYSQL_PASSWORD,
        'db' : MYSQL_DB,
        'port' : MYSQL_PORT,
        'charset' : MYSQL_CHARSET
    }
    db = pymysql.connect(**config)  # 连接数据库

    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    sql = "insert into %s (article_url,article_title,article_source) values ('%s','%s','%s')"%(table_name,articles_pool['article_url'],articles_pool['article_title'],articles_pool['article_source'])
    try:
        # 执行sql语句
        cursor.execute(sql)
        # 执行sql语句
        db.commit()
    except:
        # 发生错误时回滚
        db.rollback()
    cursor.close  # 关闭数据库连接
    db.close

# 创建一个新表
def new_table(table_name):
    # 连接数据库，新建一个表
    config = {
        'host': MYSQL_URL,
        'user': MYSQL_USER,
        'password': MYSQL_PASSWORD,
        'db': MYSQL_DB,
        'port': MYSQL_PORT,
        'charset': MYSQL_CHARSET
    }
    db = pymysql.connect(**config)  # 连接数据库

    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()

    # 获取数据库中所有表名
    cursor.execute('SHOW TABLES')
    print(cursor.fetchall())
    # 使用 execute() 方法执行 SQL，如果表存在则删除
    cursor.execute("DROP TABLE IF EXISTS %s" % (table_name))
    # 如果表已存在就不再新建

    # 使用预处理语句创建表
    sql = """CREATE TABLE %s (
        article_url text,
        article_title text,
        article_source text
        )""" % (table_name)
    cursor.execute(sql)

    cursor.close
    db.close()


def parse_sougo_page_index(text):
    try:
        soup = BeautifulSoup(text)
    except Exception:
        pass


# -----------------存储网站链接的集合--------------------------
xinhuawang = []
xinlang = []
souhuxinwen = []
zhongguojingjiwang = []


# ---------------------------------------------------------------


def news_soruce_classify(article):
    if article['article_source'] == '新华网':
        xinhuawang.append(article)
    elif article['article_source'] == '新浪':
        xinlang.append(article)
    elif article['article_source'] == '中国经济网':
        zhongguojingjiwang.append(article)




def main(keyword,pn):
    articles = get_baidu_page_index(keyword,pn=pn*10)
    articles_pool(articles)
    articles = get_sina_page_index(keyword,page=(pn+1))
    articles_pool(articles)
    articles = get_sougo_page_index(keyword,page=(pn+1))
    articles_pool(articles)





if __name__ == '__main__':
    for key,value in keyword.items():
        articles_pool_list = []
        for i in range(1):
            print(key,i)
            main(key,i)
            # print('新浪新闻：', xinlang)
            # print(len(xinlang))
            # print('新华网：', xinhuawang)
            # print(len(xinhuawang))
            # print('中国经济网：', zhongguojingjiwang)
            # print(len(zhongguojingjiwang))
        articles = pd.DataFrame(articles_pool_list)
        print(len(articles))
        articles.drop_duplicates(['article_url'])
        unique = articles['article_source'].unique()
        print(unique)
        print(len(unique))
        print(len(articles))
        if i == 0:
            print('creat new %s'%(value))
            new_table(value)
        # print(articles_pool_list[1])
        # print(articles[['article_url']])

        for j in range(len(articles)):
            save_to_mysql(value, articles.iloc[j])



