#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Created by yaochao on 2017/8/3
import sqlite3

connection = sqlite3.connect('data.sqlite')
cursor = connection.cursor()
connection.commit()


def get_then_change_latest_url(site_name, first_url):
    '''
    获取上一次网站的最新的文章的url,然后把新抓的第一条数据替换为最新的数据，存储到sqlite中
    :param name: 网站英文名称
    :param first_url: 本次抓取的第一条数据的url
    :return: 
    '''
    cursor.execute('CREATE TABLE IF NOT EXISTS {} (latest_url TEXT)'.format(site_name) )

    cursor.execute('SELECT latest_url FROM {}'.format(site_name))
    latest_url = cursor.fetchone()
    if latest_url:
        latest_url = latest_url[0]
    cursor.execute('DELETE FROM {}'.format(site_name))
    cursor.execute('INSERT INTO {} (latest_url) VALUES ("{}")'.format(site_name, first_url))
    connection.commit()
    return latest_url
