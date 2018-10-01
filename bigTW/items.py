# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy
'''
文章分類、文章標題、文章內容、圖片連結、發文時間
'''
class BigtwItem(scrapy.Item):
    # name = scrapy.Field()
    id = scrapy.Field()
    kind = scrapy.Field()
    title = scrapy.Field()
    content = scrapy.Field()
    img = scrapy.Field()
    time = scrapy.Field()
    
