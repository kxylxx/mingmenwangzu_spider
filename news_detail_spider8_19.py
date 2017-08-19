import datetime
import pymysql
import pandas as pd
import numpy as np
import re
import itertools
from requests.exceptions import RequestException
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from pyquery import PyQuery as pq
import requests
import json
from config import *


def read_from_mysql(table_name):
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
    sql = """select * from %s """ %(table_name)
    df = pd.read_sql(sql, con=db)
    return df

news_source = ['新浪', '新浪财经', '搜狐新闻', '搜狐财经', '网易新闻', '网易财经', '腾讯新闻', '腾讯财经', '人民日报网',
               '新华网', '凤凰网', '人民网', '环球网', '中国经济网', '今日头条', '中国网', '中金在线', '东方财富网',
               '和讯网', '一点资讯', '中国江苏网', '江苏新闻网', '南风窗', '扬子晚报']
with open('zhouhaijiang_newslist.txt', 'r', encoding = 'utf-8') as f:
    print(f.read())
    zhou_news_source = f.read()


def classify_news(df, news_source):
    single_source = df[(df['article_source'] == news_source[0]) | (df['article_source'] == news_source[1])
                       | (df['article_source'] == news_source[2]) | (df['article_source'] == news_source[3])]
    return single_source


# browser = webdriver.Firefox()
# wait = WebDriverWait(browser, 10)

##评论网址
commentURL = "http://comment5.news.sina.com.cn/page/info?version=1&format=js&\
channel={chs}&newsid=comos-{ids}&group=&compress=0&\
ie=utf-8&oe=utf-8&page={n}&page_size=20"


# 获取文章评论数
def get_comment_count(newsurl, restext):
    try:
        ch = re.search(r"channel: \'(.{2})\'", restext)
        channal = ch.group(1)
        m = re.search('doc-i(.*).shtml', newsurl)
        newsid = m.group(1)
        comments = requests.get(commentURL.format(chs=channal, ids=newsid, n=1))
        # print(commentURL.format(chs=channal, ids=newsid, n=1))
        jdata = json.loads(comments.text.strip('var data='))
        return jdata['result']['count']['total']
    except Exception:
        return 0


# 获取文章评论内容
def getcomments(comments_text, newsurl, restext):
    try:
        ch = re.search(r"channel: \'(.{2})\'", restext)
        channal = ch.group(1)
        m = re.search('doc-i(.*).shtml', newsurl)
        newsid = m.group(1)
        urls = (commentURL.format(chs=channal, ids=newsid, n=x) for x in itertools.count(start=1, step=1))
    except Exception:
        return ''

    for url in urls:
        try:
            comments = requests.get(url)
            comments.raise_for_status()
            jdata = json.loads(comments.text.strip('var data='))
            for jd in jdata['result']['cmntlist']:
                comments_text.append(jd['content'])
                # print(jd['content'])
        except Exception:
            break


def paras_sinanews_detail(url):
    # try:
    results = {}
    comments_text = []
    try:
        response = requests.get(url, timeout=10)
        response.encoding = response.apparent_encoding
    except RequestException:
        return None
    if response.status_code == 200:
        # browser.get(url)
        print('解析网址为：', url)
        # html = browser.page_source
        html = response.text
        doc = pq(html)
        try:
            results['article_title'] = doc('.page-header #artibodyTitle').text()
            if not results['article_title']:
                results['article_title'] = doc('#artibodyTitle').text()
            # print('article_title:', results['article_title'])
        except Exception as e:
            results['article_title'] = ''
        try:
            article_body = doc('#articleContent #artibody p').items()
            results['article_body'] = '\n'.join([p.text().strip() for p in article_body])
            if not results['article_body']:
                doc('.Main #artibody p:last-child').remove()
                article_body = doc('.Main #artibody p').items()
                results['article_body'] = '\n'.join([p.text().strip() for p in article_body])
            # print('body:', results['article_body'])
        except:
            results['article_body'] = ''
        try:
            dtt1 = doc('.page-info .time-source').text()
            print('detatime:', dtt1)
            if not dtt1:
                dtt1 = doc('.artInfo #pub_date').text()
                print('detatime:', dtt1)
            elif not dtt1:
                dtt1 = doc('#artibodyTitle .from_info').text()
            if dtt1:
                dtt2 = re.search(re.compile('(\d{4}[\u4e00-\u9fa5]\d{2}[\u4e00-\u9fa5]\d{2}[\u4e00-\u9fa5])\\xa0*(\d{2}:\d{2})', re.S), dtt1).group(1)
                dtt3 = re.search(
                    re.compile('(\d{4}[\u4e00-\u9fa5]\d{2}[\u4e00-\u9fa5]\d{2}[\u4e00-\u9fa5])\\xa0*(\d{2}:\d{2})', re.S),
                    dtt1).group(2)
                dtt4 = dtt2 + dtt3
                results['datetime'] = datetime.datetime.strptime(dtt4, '%Y年%m月%d日%H:%M')
            print('datetime:', results['datetime'])
        except:
            results['datetime'] = None
        try:
            news_url = doc("[data-sudaclick='media_name'] a").attr('href')
            if not news_url:
                news_url = doc("#media_name a:first-child").attr('href')
            if not news_url:
                news_url = url
            results['news_url'] = news_url
            # print('news_url:', news_url)
        except:
            results['news_url'] = ''
        try:
            news_realsource = doc("[data-sudaclick='media_name'] a").text()
            if not news_realsource:
                news_realsource = re.search(re.compile('([\u4e00-\u9fa5]*?)$', re.S), dtt1).group(1)
            elif not news_realsource:
                news_realsource = doc("#media_name a").text()
            # print('news_realsource:', news_realsource)
            results['news_source'] = news_realsource
        except:
            results['news_source'] = ''
        try:
            picture_url = doc('.article#artibody img').items()
            pictures_url = '\n'.join([url.attr('src') for url in picture_url])
            results['pictures_url'] = pictures_url
            # print('pictures_url:', pictures_url)#也可以加download图片的程序
        except Exception:
            results['pictures_url'] = ''
        try:
            editor = doc('.article-editor').text()
            if editor:
                article_editor = re.search(re.compile(r'([\u4e00-\u9fa5]*?)(:|：)\s*(.*?$)', re.S), editor).group(3)
            else:
                # article_editor = re.search(re.compile(r'(（|\()([\u4e00-\u9fa5]*?)\s*(:|：)\s*(.*?$)\)', re.S), doc('#articleContent').text()).group(3)
                article_editor = ''
            results['editor'] = article_editor
            # print('article_editor:', editor)
            # print('article_editor:', article_editor)
        except Exception:
            results['editor'] = ''
        try:
            comments_count = get_comment_count(url, html)
            results['comments_count'] = str(comments_count)
            # print('comments_count:', comments_count)
        except Exception:
            results['comments_count'] = ''
        try:
            getcomments(comments_text, url, html)
            results['comments_text'] = comments_text
            # print('comments_text:', comments_text)
        except Exception:
            results['comments_text'] = ''
        return results
    # except TimeoutException:
    else:
        # 保存关键信息
        return None
    # browser.close()


def save_to_mysql(table_name, single_source):
    for url in single_source['article_url']:
        result = paras_sinanews_detail(url)
        if result:
            if result['datetime']:
                # print(result)
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
                sql = "insert into {table_name} (article_title, article_body, datetime, editor, news_source, news_url, pictures_url, comments_count, comments_text) values (%s, %s, %s, %s, %s, %s, %s, %s, %s)".format(table_name=table_name)
                # print('sql:', sql)
                args = (result['article_title'], result['article_body'], result['datetime'], result['editor'], result['news_source'], result['news_url'], result['pictures_url'], result['comments_count'], '\n'.join(result['comments_text']))
                try:
                    # 执行sql语句
                    cursor.execute(sql, args=args)
                    # 执行sql语句
                    db.commit()
                    print('插入成功')
                except Exception as e:
                    print(e)
                    # 发生错误时回滚
                    print('插入失败')
                    db.rollback()
    db.close()  # 关闭数据库连接


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
        id int primary key AUTO_INCREMENT,
        article_title text,
        article_body text,
        datetime datetime,
        editor text, 
        news_source text,
        news_url text,
        pictures_url text,
        comments_count varchar(10),
        comments_text text
        )""" % (table_name)
    cursor.execute(sql)
    db.close()


def main(table_name, table_name_detail):
    df = read_from_mysql(table_name)
    print(news_source[0])
    # print(df['article_source'][:])
    single_source = classify_news(df, ['新浪', '新浪财经', '新浪新闻', '新浪教育', '新浪无锡站', '新浪综合'])
    print(len(single_source))
    save_to_mysql(table_name_detail, single_source)
    # print(df.head())
    print(type(df))


if __name__ == '__main__':

    for keyword, table_name in news_list.items():
        new_table(news_detail[keyword])
        main(table_name, news_detail[keyword])
