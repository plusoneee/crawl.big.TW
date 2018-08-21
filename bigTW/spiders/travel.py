#!/usr/local/bin/python3
# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors import LinkExtractor
from ..items import BigtwItem
import re
import pymysql.cursors
class TravelSpider(scrapy.Spider):
    name = 'travel'
    allowed_domains = ['blog.tranews.com']
    num = 1
    start_urls = ['http://blog.tranews.com/blog/category/%E6%97%85%E9%81%8A',
                  'http://blog.tranews.com/blog/%E7%BE%8E%E9%A3%9F',
                  'http://blog.tranews.com/blog/%E8%97%9D%E6%96%87',
                  'http://blog.tranews.com/blog/%E4%BC%91%E9%96%92']
    def parse(self, response):
        if response.status == 200:
            le = LinkExtractor(restrict_css='article.post div.entry').extract_links(response)
            for link in le:
                if '/blog/' in link.url: 
                    yield scrapy.Request(link.url, callback=self.article_parser)
            self.num += 1
            next_page = self.start_urls[0] +'/page/'+ str(self.num)
            yield scrapy.Request(next_page, callback=self.parse)
        else:
            print('Done!')
    
    def article_parser(self, response):
        article = BigtwItem()
        sel = response.css('div.entry')
        article['title'] = sel.css('h1.entry-title::text').extract_first()
        article['postTime'] = sel.css('span.entry-date::text').extract_first()
        article['category'] = sel.css('span.entry-category a::text').extract_first()
        article['imgUrl'] = sel.xpath('//img/@src').extract_first()
        content = sel.css('div.entry-content p::text').extract()[1:]
        article['content'] = ','.join(content)
        return article
