# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.exporters import JsonLinesItemExporter
import pymysql
from pymysql import cursors
from fang.items import ESFHouseItem,NewHouseItem


class FangSqlPipeline:
    def __init__(self):
        dbparams = {
            'host': '127.0.0.1',
            'port': 3306,
            'user': 'root',
            'password': '34808',
            'db': 'fang',
            'charset': 'utf8'
        }
        self.conn = pymysql.connect(**dbparams)
        self.cursor = self.conn.cursor()
        self._sql = None
        
    def process_item(self,item,spider):
        if isinstance(item,NewHouseItem):
            self.cursor.execute(self.new_sql, (item['province'], item['city'],
                                           item['name'], item['rooms'],
                                           item['address'],item['sale'],
                                           item['area'], item['price'],
                                           item['district'], item['origin_url']))
            self.conn.commit()
            self._sql = None
            return item

        else:
            self.cursor.execute(self.esf_sql, (item['province'],item['city'],
                                           item['name'],item['rooms'],
                                           item['floor'],item['toward'],
                                           item['year'],item['address'],
                                           item['area'],item['price'],
                                           item['unit'],item['origin_url']))
            self.conn.commit()
            self._sql = None
            return item

    @property
    def esf_sql(self):
        if not self._sql:
            self._sql = """
                        insert into esf
                        values
                        (null, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
            return self._sql
        return self._sql

    @property
    def new_sql(self):
        if not self._sql:
            self._sql = """
                            insert into new
                            values
                            (null, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
            return self._sql
        return self._sql


class FangPipeline:
    def __init__(self):
        self.newhouse_fp = open('newhouse.json','wb')
        self.esfhouse_fp = open('esfhouse.json','wb')
        self.newhouse_exporter = JsonLinesItemExporter(
            self.newhouse_fp,ensure_ascii=False
        )
        self.esfhouse_exporter = JsonLinesItemExporter(
            self.esfhouse_fp,ensure_ascii=False
        )
        
    def process_item(self, item, spider):
        self.newhouse_exporter.export_item(item)
        self.esfhouse_exporter.export_item(item)
        return item

    def close_spider(self,spider):
        self.newhouse_fp.close()
        self.esfhouse_fp.close()