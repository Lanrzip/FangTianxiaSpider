# -*- coding: utf-8 -*-
import scrapy
import re
from fang.items import NewHouseItem,ESFHouseItem
from scrapy_redis.spiders import RedisSpider


class SfwSpider(RedisSpider):
    name = 'sfw'
    allowed_domains = ['fang.com']
    # start_urls = ['https://www.fang.com/SoufunFamily.htm']
    redis_key = "fang:start_urls"

    def parse(self, response):
        trs = response.xpath("//div[@class='outCont']//tr")
        province = None
        for tr in trs:
            tds = tr.xpath(".//td[not(@class)]")
            province_td = tds[0]
            province_text = province_td.xpath(".//text()").get()
            province_text = re.sub(r"\s",'',province_text)
            if province_text:
                province = province_text
            # 跳过海外房子
            if province == "其它":
                continue
            city_td = tds[1]
            city_links = city_td.xpath(".//a")
            for city_link in city_links:
                city = city_link.xpath(".//text()").get()
                city_url = city_link.xpath(".//@href").get()
                # 新房url
                url_model = city_url.split("fang")
                prefix = url_model[0]
                suffix = url_model[1]
                newhouse_url = prefix + "newhouse." + "fang" + suffix + "house/s/"
                # 二手房url
                esf_url = prefix + "esf." + "fang" + suffix

                # yield scrapy.Request(url=newhouse_url,callback=self.parse_newhouse,
                #                      meta={'info':(province,city)})

                yield scrapy.Request(url=esf_url,callback=self.parse_esf,
                                     meta={'info':(province,city)})



    def parse_newhouse(self,response):
        province, city = response.meta.get('info')
        lis = response.xpath("//div[@class='nhouse_list']//ul/li")
        for li in lis:
            name = li.xpath(".//div[@class='nlcd_name']/a/text()").get()
            if name:
                name = name.strip()
            house_type_list = li.xpath(".//div[contains(@class,'house_type')]/a/text()").getall()
            rooms = list(map(lambda x: re.sub(r"\s","",x), house_type_list))
            area = "".join(li.xpath(".//div[contains(@class,'house_type')]/text()").getall())
            area = re.sub(r"\s|－|/","",area)
            address = li.xpath(".//div[@class='address']/a/@title").get()
            district_text = "".join(li.xpath(".//div[@class='address']/a//text()").getall())
            district = re.search(r".*\[(.+)\].*",district_text).group(1)
            sale = li.xpath(".//div[contains(@class,'fangyuan')]/span/text()").get()
            price_text = "".join(li.xpath(".//div[@class='nhouse_price']//text()").getall())
            price = re.sub(r"\s","",price_text)
            origin_url = "http:" + li.xpath(".//div[@class='nlcd_name']/a/@href").get()

            item = NewHouseItem(
                                province=province,
                                city=city,
                                name=name,
                                rooms=rooms,
                                area=area,
                                address=address,
                                district=district,
                                sale=sale,
                                price=price,
                                origin_url=origin_url,)
            yield item

        next_url = response.xpath("//div[@class='page']//a[@class='next']/@href").get()
        if next_url:
            yield scrapy.Request(url=response.urljoin(next_url),callback=self.parse_newhouse,
                                 meta={'info':(province,city)})

    def parse_esf(self,response):
        province, city = response.meta.get('info')
        dls = response.xpath("//div[contains(@class,'shop_list')]/dl")
        for dl in dls:
            item = ESFHouseItem(province=province,city=city)
            name = dl.xpath(".//p[@class='add_shop']/a/text()").get()
            if name:
                name = name.strip()
            item['name'] = name
            infos = dl.xpath(".//p[@class='tel_shop']/text()").getall()
            infos = list(map(lambda x:re.sub(r"\s","",x),infos))
            for info in infos:
                if "厅" in info:
                    item['rooms'] = info
                elif "层" in info:
                    item['floor'] = info
                elif "向" in info:
                    item['toward'] = info
                elif "㎡" in info:
                    item['area'] = info
                elif "年" in info:
                    item['year'] = info
            address = dl.xpath(".//p[@class='add_shop']/span/text()").get()
            item['address'] = address
            price = dl.xpath(".//dd[@class='price_right']/span[@class='red']//text()").getall()
            price = "".join(list(map(lambda x:re.sub(r"\s","",x),price)))
            unit = dl.xpath(".//dd[@class='price_right']/span[2]/text()").get()
            item['price'] = price
            item['unit'] = unit
            suffix_url = dl.xpath(".//h4[@class='clearfix']/a/@href").get()
            item['origin_url'] = response.urljoin(suffix_url)

            yield item

        next_url = response.xpath("//div[@class='page_box']/p[3]/a/@href").get()
        if next_url:
            yield scrapy.Request(url=response.urljoin(next_url),callback=self.parse_esf,
                                 meta={'info':(province,city)})
