import re
from time import sleep
from urllib.parse import urlencode
import pypinyin
from pypinyin import pinyin, lazy_pinyin
import requests
import pandas as pd
import numpy as np
import pymysql
from bs4 import BeautifulSoup
from pyquery import PyQuery as pq
from config import *



def get_baidu_page_index(keyword,page):
    data = {
        'word': keyword,
        'pn': page,
        'tn': 'news'
    }
    params = urlencode(data)
    url = 'http://news.baidu.com/ns?' + params
    doc = pq(url)
    news_lists = doc('#wrapper_wrapper #content_left div.result').items()
    page_number = doc('p#page strong span.pc').text()
    print('page', int(page/10+1), 'page_number', page_number)
    if int(page_number) == int(page/10+1):
        if news_lists:
            for news in news_lists:
                yield {
                    'article_url': news.find('.c-title a').attr('href'),
                    'article_title': news.find('.c-title a').text(),
                    'article_source': re.search(re.compile('(.*?)\\xa0', re.S), news.find('p.c-author').text()).group(1)
                }
    else:
        yield None


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
    # print(doc)
    news_lists = doc('div#result.result .box-result.clearfix').items()
    page_number = doc('div#result.result .pagebox span').text()
    # 在获取page_number时有时候一次获取不到，加上下面判断一次获取不到就多获取几次，这样程序更健壮
    if not page_number:
        page_number = doc('div#result.result .pagebox span').text()
    elif not page_number:
        page_number = doc('div#result.result .pagebox span').text()
    print('page', page, 'page_number', page_number)
    if int(page_number) == page:
        if news_lists:
            for news in news_lists:
                yield {
                    'article_url': news.find('a').attr('href'),
                    'article_title': news.find('a').text(),
                    # 'article_source': news.find('span.fgray_time').text()
                    'article_source': re.search(re.compile('([\u4e00-\u9fa5]*?) \d', re.S),
                                                news.find('span.fgray_time').text()).group(1)
                }
    else:
        yield None

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
    page_number = doc('.p#pagebar_container span').text()
    print('page', page, 'page_number', page_number)
    if int(page_number) == page:
        if news_lists:
            for news in news_lists:
                yield {
                    'article_url': news.find('.vrTitle a').attr('href'),
                    'article_title': news.find('.vrTitle a').text(),
                    # 'article_source': news.find('.news-detail .news-from').text()
                    'article_source': re.search(re.compile('([\u4e00-\u9fa5]*?)\\xa0', re.S),
                                                news.find('.news-detail .news-from').text()).group(1)
                }
    else:
        yield None

def articles_pool(articles):
    i = 0
    for article in articles:
        i += 1
        if article:
            articles_pool_list.append(article)
    return i



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
    for j in range(len(articles_pool)):
        sql = "insert into %s (article_url,article_title,article_source) values ('%s','%s','%s')"%(table_name,articles_pool[j]['article_url'],articles_pool[j]['article_title'],articles_pool[j]['article_source'])
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


    # 使用预处理语句创建表
    sql = """CREATE TABLE %s (
        article_url text,
        article_title text,
        article_source text
        )""" % (table_name)
    cursor.execute(sql)

    cursor.close
    db.close()



def main(keyword,pn):
    # articles = get_baidu_page_index(keyword, page=(200))
    # addnum = articles_pool(articles)
    # print('addnum', addnum)

    for i in range(pn):
        print(keyword,i)
        articles = get_baidu_page_index(keyword,page=(i*10))
        # print(articles)
        add_num = articles_pool(articles)
        if add_num == 1:
            break


    for j in range(pn):
        print(keyword, j)
        # sleep(2)
        articles = get_sina_page_index(keyword,page=(j+1))
        add_num = articles_pool(articles)
        if add_num == 1:
            break


    for k in range(pn):
        print(keyword, k)
        articles = get_sougo_page_index(keyword,page=(k+1))
        add_num = articles_pool(articles)
        if add_num == 1:
            break


if __name__ == '__main__':
    for key,value in keyword.items():
        articles_pool_list = []
        main(key,PAGE_NUMBER)
        # print(articles_pool_list[-5:])
        articles = pd.DataFrame(articles_pool_list)
        print(len(articles))
        articles.drop_duplicates(['article_url'])
        unique = articles['article_source'].unique()
        print(unique)
        print(len(unique))
        print(len(articles))
        print('creat new %s'%(value))
        # new_table(value)
        # print(articles_pool_list[1])
        # print(articles[['article_url']])
        # save_to_mysql(value, articles_pool_list)
