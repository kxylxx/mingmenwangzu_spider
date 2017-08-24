from selenium import webdriver
from pyquery import PyQuery as pq
import time
import pymysql
import pandas as pd
import numpy as np
from config_db import *
import re


#获取网页的所有内容
def get_onepoint_all_news(keyword):
    url = "http://www.yidianzixun.com/channel/w/%s?searchword=%s" %(keyword,keyword)
    brower = webdriver.Chrome()
    brower.get(url)
    # 使用for循环完整下拉加载网页中对应的所有内容，当判断找到对应的结束标签后就停止加载，输出全部网页内容
    for i in range(4):
        brower.execute_script('window.scrollTo(0, document.body.scrollHeight)')
        time.sleep(2)
        try:
            target_null = brower.find_element_by_class_name("no-news")
            if target_null:
                break
        except Exception as ex:
            pass
        try:
            target = brower.find_element_by_class_name("news-no-more")
            if target :
                break
        except Exception as e:
            print("持续加载中")
            pass
    print(keyword+"全部加载完成")
    html = brower.page_source
    doc = pq(html)
    news_list = doc('#js-main .channel-news').children()
    style_all = news_list('.item.doc').items()# 获取三类文章类型的所有链接
    if news_list:
        for new in style_all:
            yield {
                'article_url': "http://www.yidianzixun.com"+new.attr('href'),
                'article_title': new.find('.doc-title').text(),
                'article_source': new.find('.source').text(),
                'article_date': new.find('.date').text(),
                #'article_comment_count': new.find('.comment-count').text()
                'article_comment_count': re.search("(^.*)评$", new.find('.comment-count').text()).group(1)
                #'article_image': new.find('.doc-image doc-image-small').attr('scr')
            }
    brower.close()


#存储解析的内容到数据库之中
def create_database(db_name,table_name):
    # 连接数据库，新建一个数据库
    config = {
        'host' : MYSQL_URL,
        'user' : MYSQL_USER,
        'password' : MYSQL_PASSWORD,
        'port' : MYSQL_PORT,
        'charset' : MYSQL_CHARSET
    }
    conn = pymysql.connect(**config)  # 连接数据库
    cursor = conn.cursor()
    create_database = "create database if not exists %s DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci" %(db_name)
    try:
        cursor.execute(create_database)  # 创建一个新的数据库
        conn.commit()
    except Exception as e:
        print(e)
        conn.rollback()

    conn.select_db(db_name)
    sql_creatable = "CREATE TABLE IF NOT EXISTS \
                  %s (id int primary key AUTO_INCREMENT, article_comment_count VARCHAR(250), article_date datetime, " \
                    "article_source text, article_title text, article_url VARCHAR(250))  ENGINE=InnoDB DEFAULT CHARSET=utf8" %(table_name)
    try:
        cursor.execute("DROP TABLE IF EXISTS %s" % (table_name))
        cursor.execute(sql_creatable)
        conn.commit()
    except Exception as e:
        print(e)
        conn.rollback()

    cursor.close()  # 关闭游标
    conn.close()  # 释放数据库资源


def insert_date(db_name,table_name, one):
    config = {
        'host': MYSQL_URL,
        'user': MYSQL_USER,
        'password': MYSQL_PASSWORD,
        'db': db_name,
        'port': MYSQL_PORT,
        'charset': MYSQL_CHARSET
    }
    conn = pymysql.connect(**config)  # 连接数据库
    cursor = conn.cursor()
    #插入的字段
    sql_insert = "insert into %s (article_comment_count , article_date , article_source , article_title , article_url ) " \
                 "values ('%s','%s','%s','%s','%s')" % (table_name, one[0], one[1], one[2], one[3], one[4])
    try:
        # 执行sql语句
        cursor.execute(sql_insert)
        #print("插入成功")

        conn.commit()
    except Exception as e:
        print(e)
        # 发生错误时回滚
        conn.rollback()
    cursor.close() #关闭游标
    conn.close()#释放数据库资源


#database flavors mysql is not supported，这种插入数据库的方式需要再 改进一下
# def save_dataframe(db_name,articls):
#     config = {
#         'host': MYSQL_URL,
#         'user': MYSQL_USER,
#         'password': MYSQL_PASSWORD,
#         'db': db_name,
#         'port': MYSQL_PORT,
#         'charset': MYSQL_CHARSET
#     }
#     try :
#         conn = pymysql.connect(**config)  # 连接数据库
#         articls.to_sql('zhouhaijiang', conn,flavor="mysql")
#         conn.close()
#         print("插入成功")
#     except Exception as e:
#         print(e)


def main():
    for keyword, table_name in table_name_list.items():
        db_name = MYSQL_DB
        articles = get_onepoint_all_news(keyword)
    #调用函数时，先建数据库的函数 再调用插入数据表的函数
        create_database(db_name, table_name)
    #将字典型的数据转换为列表型的后通过循环调用yield值可以对应的插入数据库
        for article in np.array(pd.DataFrame(articles)):
            one = article.tolist()
            insert_date(db_name,table_name, one)
        print("bingo")
main()


