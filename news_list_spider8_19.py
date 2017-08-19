########运行程序前需要新建一个名字为news的数据库
import os
import re
from time import sleep
from urllib.parse import urlencode
import pypinyin
from pypinyin import pinyin, lazy_pinyin
import requests
from requests.exceptions import RequestException
import pandas as pd
import numpy as np
import pymysql
from bs4 import BeautifulSoup
from pyquery import PyQuery as pq

from config import *


def get_baidu_page_index(keyword, page):
    data = {
        'word': keyword,
        'pn': page,
        'tn': 'news'
    }
    params = urlencode(data)
    url = 'http://news.baidu.com/ns?' + params
    try:
        response = requests.get(url, timeout=10)
        response.encoding = response.apparent_encoding
    except RequestException:
        return None
    if response.status_code == 200:
        # print(response.text)
        doc = pq(response.text)
        # doc = pq(url)
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


def get_sina_page_index(keyword, page):
    data = {
        'q': keyword,
        'c': 'news',
        'page': page
    }
    params = urlencode(data)
    base = 'http://search.sina.com.cn/?'
    url = base + params
    try:
        response = requests.get(url, timeout=10)
        # response.encoding = response.apparent_encoding
    except RequestException:
        return None
    if response.status_code == 200:
        # print(response.text)
        doc = pq(response.text)
        # print(doc)
        news_lists = doc('div#result.result .box-result.clearfix').items()
        page_number = doc('.result .pagebox span.pagebox_cur_page').text()
        print('page', page, 'page_number', page_number)

        try:
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
        except Exception as e:
            print(e)
            return get_sina_page_index(keyword, page)


def get_sougo_page_index(keyword, page):
    data = {
        'query': keyword,
        'page': page
    }
    params = urlencode(data)
    base = 'http://news.sogou.com/news?'
    url = base + params

    try:
        response = requests.get(url, timeout=10)
        response.encoding = response.apparent_encoding
    except RequestException:
        return None
    if response.status_code == 200:
        # print(response.text)
        doc = pq(response.text)
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


def save_to_mysql(table_name, articles_pool_list):


    # 连接数据库，新建一个数据库
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

    # 创建数据库
    # create_database = "create database if not exists %s" % (news)
    # try:
    #     cur.execute(create_database)  # 创建一个新的数据库
    #     conn.commit()
    # except Exception as e:
    #     print(e)
    #     conn.rollback()
    for j in range(len(articles_pool_list)):
        sql = "insert into {table_name} (article_source,article_title,article_url) values (%s,%s,%s)".format(table_name=table_name)
        args = (articles_pool_list.iloc[j, 0], articles_pool_list.iloc[j, 1], articles_pool_list.iloc[j, 2])
        try:
            # 执行sql语句
            cursor.execute(sql, args=args)
            # 执行sql语句
            db.commit()
        except Exception as e:
            print('插入失败', e)
            # 发生错误时回滚
            db.rollback()
    db.close()  # 关闭数据库连接


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
    cursor.execute("DROP TABLE IF EXISTS %s" % table_name)
    # 使用预处理语句创建表
    sql = """CREATE TABLE %s (
        id integer primary key AUTO_INCREMENT,
        article_url text,
        article_title text,
        article_source text
        )""" % (table_name)
    cursor.execute(sql)
    db.close()


def save_news_source(unique, value):
    file_path = '{0}/{1}.{2}'.format(os.getcwd(), value, 'txt')
    print(file_path)
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(unique)
        f.close()


def main(keyword, pn):
    # articles = get_baidu_page_index(keyword, page=(200))
    # addnum = articles_pool(articles)
    # print('addnum', addnum)
    baidu_newsnum = 0
    sina_newsnum = 0
    sougo_newsnum = 0
    for i in range(pn):
        print(keyword, i)
        articles = get_baidu_page_index(keyword, page=(i*10))
        # print(articles)
        add_num = articles_pool(articles)
        if add_num == 1:
            break
        else:
            baidu_newsnum += add_num

    for j in range(pn):
        print(keyword, j)
        articles = get_sina_page_index(keyword, page=(j+1))
        add_num = articles_pool(articles)
        if add_num == 1:
            break
        else:
            sina_newsnum += add_num

    for k in range(pn):
        print(keyword, k)
        articles = get_sougo_page_index(keyword, page=(k+1))
        add_num = articles_pool(articles)
        if add_num == 1:
            break
        else:
            sougo_newsnum += add_num
    return [baidu_newsnum, sina_newsnum, sougo_newsnum]

if __name__ == '__main__':
    for keyword, table_name in news_list.items():
        articles_pool_list = []
        news_num = main(keyword, PAGE_NUMBER)
        print('news_num:', news_num)
        print(articles_pool_list)
        articles = pd.DataFrame(articles_pool_list)
        print(len(articles))
        articles = articles.drop_duplicates(['article_title'])
        articles_pool_list = articles
        unique = articles['article_source'].unique()
        # print(','.join(unique))
        # save_news_source(','.join(unique), value)
        print(len(unique))
        print(len(articles))
        print('creat new %s' % table_name)
        new_table(table_name)
        # print(articles_pool_list[1])
        # print(articles[['article_url']])
        save_to_mysql(table_name, articles_pool_list)
