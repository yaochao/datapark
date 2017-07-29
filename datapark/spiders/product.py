# -*- coding: utf-8 -*-
import json
import sqlite3
import time

import scrapy
from scrapy import Request, Selector


class Kr36Spider(scrapy.Spider):
    name = 'kr36'
    allowed_domains = ['36kr.com']
    start_urls = ['http://36kr.com/']
    custom_settings = {
        'ITEM_PIPELINES': {
            'datapark.pipelines.BrandMongoPipeline': 300,
            'datapark.pipelines.BrandKafkaPipeline': 301,
        }
    }

    # 自定义属性
    first_url = ''
    connection = sqlite3.connect('data.sqlite')
    cursor = connection.cursor()
    cursor.execute('CREATE TABLE IF NOT EXISTS %s (latest_url TEXT)' % name)
    connection.commit()
    cursor.execute('SELECT latest_url FROM %s' % name)
    latest_url = cursor.fetchone()
    if latest_url:
        latest_url = latest_url[0]

    def parse(self, response):
        script_text = response.xpath('//*[@id="app"]/following::*[1]/text()').extract_first()
        props = script_text.split(',locationnal')[0].split('var props=')[-1]
        props = json.loads(props)
        first_post_id = props['feedPostsLatest|post'][0]['id']
        request = Request(
            url='http://36kr.com/api/info-flow/main_site/posts?b_id={}&per_page=200'.format(first_post_id),
            callback=self.parse_ajax)
        yield request

    def parse_ajax(self, response):
        res_json = json.loads(response.text)
        if res_json['code'] == 0:
            posts = res_json['data']['items']
            for post in posts:
                post_url = 'http://36kr.com/p/{}.html'.format(post['id'])
                post_title = post['title']
                item = {
                    '_id': post_url,
                    'post_url': post_url,
                    'post_title': post_title
                }

                # 把第一条数据作为最新的数据，存储到sqlite中
                if not self.first_url:
                    self.first_url = post_url
                    self.cursor.execute('DELETE FROM %s' % self.name)
                    self.cursor.execute('INSERT INTO %s (latest_url) VALUES ("%s")' % (self.name, self.first_url))
                    self.connection.commit()
                # 从sqlite中取出上一次最新的数据，与本次的数据做对比，如果相同则认为文章抓到了上次已经抓过的数据，如果不同则认为文章还没有抓完
                if post_url == self.latest_url:
                    print '%s - 爬到了上次爬到的地方' % self.name
                    self.connection.close()
                    return

                request = scrapy.Request(url=post_url, callback=self.parse_post)
                request.meta['item'] = item
                yield request

    def parse_post(self, response):
        item = response.meta['item']
        script_text = response.xpath('//*[@id="app"]/following::*[1]/text()').extract_first()
        props = script_text.split(',locationnal')[0].split('var props=')[-1]
        props = json.loads(props)
        content_html = props['detailArticle|post']['content']
        content_text = Selector(text=content_html).xpath(
            '//text()[normalize-space() and not(ancestor::script | ancestor::style)]').extract()
        content_text = ''.join(content_text)

        item['content_text'] = content_text.replace('\r', '').replace('\n', '').replace('\t', '')
        item['content_html'] = content_html
        item['crawl_time'] = int(time.time())
        item['site_name'] = self.name
        item['type'] = 'commerce'
        item['module'] = 'product'
        yield item
