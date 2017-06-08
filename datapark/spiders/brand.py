# -*- coding: utf-8 -*-
import time

import scrapy
import sqlite3


class SocialbetaSpider(scrapy.Spider):
    name = 'socialbeta'
    allowed_domains = ['socialbeta.com']
    start_urls = ['http://socialbeta.com/tag/案例']
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
        posts = response.xpath('//*[@class="postimg"]/li')
        for post in posts:
            post_url = response.urljoin(post.xpath('div/div/h3/a/@href').extract_first())
            post_title = post.xpath('div/div/h3/a/text()').extract_first()
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
        content_text = response.xpath('//div[@class="content"]').xpath('string(.)').extract_first()
        content_html = response.xpath('//div[@class="content"]').extract_first()
        item['content_text'] = content_text.strip() if content_text else None
        item['content_html'] = content_html
        item['crawl_time'] = int(time.time())
        item['site_name'] = self.name
        item['type'] = 'brand'
        yield item


class QdailySpider(scrapy.Spider):
    name = 'qdaily'
    allowed_domains = ['qdaily.com']
    start_urls = ['http://www.qdaily.com/categories/18.html/']
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
        posts = response.xpath('//*[@class="packery-container articles"]/div')
        for post in posts:
            post_url = response.urljoin(post.xpath('a/@href').extract_first())
            post_title = post.xpath('a/div/div/img/@alt').extract_first()
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
        # 文章的布局分为两种情况，根据文章中的元素做出解析(xpath)的选择。
        if response.xpath('//div[@class="main long-article"]'):
            content_text = response.xpath('//div[@class="main long-article"]').xpath('string(.)').extract_first()
            content_html = response.xpath('//div[@class="main long-article"]').extract_first()
        else:
            content_text = response.xpath('//div[@class="article-detail-bd"]').xpath('string(.)').extract_first()
            content_html = response.xpath('//div[@class="article-detail-bd"]').extract_first()
        item['content_text'] = content_text.strip() if content_text else None
        item['content_html'] = content_html
        item['crawl_time'] = int(time.time())
        item['site_name'] = self.name
        item['type'] = 'brand'
        yield item


class JiemianSpider(scrapy.Spider):
    name = 'jiemian'
    start_urls = ['http://www.jiemian.com/lists/49.html']
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
        posts = response.xpath('//*[@id="load-list"]/div')
        for post in posts:
            post_url = response.urljoin(post.xpath('div[@class="news-right"]/div[@class="news-header"]/h3/a/@href').extract_first())
            post_title = post.xpath('div[@class="news-right"]/div[@class="news-header"]/h3/a/text()').extract_first()
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
        content_text = response.xpath('//div[@class="article-main"]').xpath('string(.)').extract_first()
        content_html = response.xpath('//div[@class="article-main"]').extract_first()
        item['content_text'] = content_text.strip() if content_text else None
        item['content_html'] = content_html
        item['crawl_time'] = int(time.time())
        item['site_name'] = self.name
        item['type'] = 'brand'
        yield item


class ToodaylabSpider(scrapy.Spider):
    name = 'toodaylab'
    start_urls = ['http://www.toodaylab.com/field/308']
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
        posts = response.xpath('//*[@class="content"]/div')
        for post in posts:
            post_url = response.urljoin(post.xpath('div[@class="post-info"]/p/a/@href').extract_first())
            post_title = post.xpath('div[@class="post-info"]/p/a/text()').extract_first()
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
        content_text = response.xpath('//div[@class="post-content"]').xpath('string(.)').extract_first()
        content_html = response.xpath('//div[@class="post-content"]').extract_first()
        item['content_text'] = content_text.strip() if content_text else None
        item['content_html'] = content_html
        item['crawl_time'] = int(time.time())
        item['site_name'] = self.name
        item['type'] = 'brand'
        yield item


class MadisonboomSpider(scrapy.Spider):
    name = 'madisonboom'
    start_urls = ['http://www.madisonboom.com/category/works/']
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
        posts = response.xpath('//*[@id="gallery_list_elements"]/li')
        for post in posts:
            post_url = response.urljoin(post.xpath('h3/a/@href').extract_first())
            post_title = post.xpath('h3/p/a/text()').extract_first()
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
        content_text = response.xpath('//div[@class="slide-info"]').xpath('string(.)').extract_first()
        content_html = response.xpath('//div[@class="slide-info"]').extract_first()
        item['content_text'] = content_text.strip() if content_text else None
        item['content_html'] = content_html
        item['crawl_time'] = int(time.time())
        item['site_name'] = self.name
        item['type'] = 'brand'
        yield item


class IwebadSpider(scrapy.Spider):
    name = 'iwebad'
    start_urls = ['http://iwebad.com/']
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
        posts = response.xpath('//*[@class="new_search_works"]/div')
        for post in posts:
            post_url = response.urljoin(post.xpath('div[@class="works_info"]/h4/span/a/@href').extract_first())
            post_title = post.xpath('div[@class="works_info"]/h4/span/a/text()').extract_first()
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
        content_text = response.xpath('//div[@class="news_ckkk "]').xpath('string(.)').extract_first()
        content_html = response.xpath('//div[@class="news_ckkk "]').extract_first()
        item['content_text'] = content_text.strip() if content_text else None
        item['content_html'] = content_html
        item['crawl_time'] = int(time.time())
        item['site_name'] = self.name
        item['type'] = 'brand'
        yield item


class AdquanSpider(scrapy.Spider):
    name = 'adquan'
    start_urls = ['http://www.adquan.com/']
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
        posts = response.xpath('//*[@class="work_list_left"]/div')
        for post in posts:
            post_url = response.urljoin(post.xpath('h2/a/@href').extract_first())
            post_title = post.xpath('h2/a/text()').extract_first()
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
        content_text = response.xpath('//div[@class="deta_inner"]').xpath('string(.)').extract_first()
        content_html = response.xpath('//div[@class="deta_inner"]').extract_first()
        if not content_text:
            content_text = response.xpath('//div[@class="con_Text"]').xpath('string(.)').extract_first()
            content_html = response.xpath('//div[@class="con_Text"]').extract_first()
        item['content_text'] = content_text.strip() if content_text else None
        item['content_html'] = content_html
        item['crawl_time'] = int(time.time())
        item['site_name'] = self.name
        item['type'] = 'brand'
        yield item


class DigitalingSpider(scrapy.Spider):
    name = 'digitaling'
    start_urls = ['http://www.digitaling.com/projects']
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
        posts = response.xpath('//div[@id="pro_list"]/div')
        for post in posts:
            post_url = response.urljoin(post.xpath('div[@class="works_bd"]/div/h3/a/@href').extract_first())
            post_title = post.xpath('div[@class="works_bd"]/div/h3/a/text()').extract_first()
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
        content_text = response.xpath('//div[@id="article_con"]').xpath('string(.)').extract_first()
        content_html = response.xpath('//div[@id="article_con"]').extract_first()
        item['content_text'] = content_text.strip() if content_text else None
        item['content_html'] = content_html
        item['crawl_time'] = int(time.time())
        item['site_name'] = self.name
        item['type'] = 'brand'
        yield item



class IresearchSpider(scrapy.Spider):
    name = 'iresearch'
    start_urls = ['http://a.iresearch.cn/']
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
        posts = response.xpath('//div[@id="tab-list"]/div/ul/li')
        for post in posts:
            post_url = response.urljoin(post.xpath('h3/a/@href').extract_first())
            post_title = post.xpath('h3/a/text()').extract_first()
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
        content_text = response.xpath('//div[@class="m-article"]').xpath('string(.)').extract_first()
        content_html = response.xpath('//div[@class="m-article"]').extract_first()
        item['content_text'] = content_text.strip() if content_text else None
        item['content_html'] = content_html
        item['crawl_time'] = int(time.time())
        item['site_name'] = self.name
        item['type'] = 'brand'
        yield item



class EbrunSpider(scrapy.Spider):
    name = 'ebrun'
    start_urls = ['http://www.ebrun.com/brands/']
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
        posts = response.xpath('//div[@id="create10"]/div/div')
        for post in posts:
            post_url = response.urljoin(post.xpath('p/span/a/@href').extract_first())
            post_title = post.xpath('p/span/a/text()').extract_first()
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
        content_text = response.xpath('//div[@class="clearfix cmsDiv"]').xpath('string(.)').extract_first()
        content_html = response.xpath('//div[@class="clearfix cmsDiv"]').extract_first()
        item['content_text'] = content_text.strip() if content_text else None
        item['content_html'] = content_html
        item['crawl_time'] = int(time.time())
        item['site_name'] = self.name
        item['type'] = 'brand'
        yield item