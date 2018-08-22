# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql

class BigtwPipeline(object):
    def process_item(self, item, spider):
        return item
class MySqlPipeline(object):
    old_data_from_sql = []
    def open_spider(self, spider):

        # Database Settings
        db = spider.settings.get('MYSQL_DB_NAME')
        host = spider.settings.get('MYSQL_DB_HOST')
        port = spider.settings.get('MYSQL_PORY')
        user = spider.settings.get('MYSQL_USER')
        password = spider.settings.get('MYSQL_PASSWORD')
        # Database Connecting
        self.connection = pymysql.connect(
            host = host,
            user = user,
            password= password,
            db = db,
            cursorclass= pymysql.cursors.DictCursor
        )
        
    def close_spider(self, spider):
        self.connection.close()
    
    def process_item(self, item, spider):
        self.filter_repeat_data(item)
        return item

    def filter_repeat_data(self, item):
        # Getting Old Data from DB
        with self.connection.cursor() as cursor:
            sql = "SELECT * FROM newsAll"
            cursor.execute(sql)
            for row in cursor:
                self.old_data_from_sql.append(row['title'])
        
        if item['title'] not in self.old_data_from_sql:
            self.insert_to_mysql(item)

    def insert_to_mysql(self, item):
        values = (
            item['title'],
            item['content'],
            item['category'],
            item['imgUrl'],
            item['postTime'],
        )
        with self.connection.cursor() as cursor:
            sql = 'INSERT INTO `newsAll` (`title`, `content`, `category`, `imgUrl`, `postTime`) VALUES (%s, %s, %s, %s, %s)'
            cursor.execute(sql,(
                item['title'],
                item['content'],
                item['category'],
                item['imgUrl'],
                item['postTime'],)
            )
            self.connection.commit()
            print('Data Aready Insert to DB')
            