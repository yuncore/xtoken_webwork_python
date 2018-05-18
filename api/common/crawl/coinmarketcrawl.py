from api.db import crawl_db, CrawlCollections
from api.common.utils.util import str_to_float, str_to_int
from api.common.crawl.scrapyd import scrapy_add_job, scrapy_list_job, scrapy_cancel_job
import requests


class CoinMarketCrawl:

    def crawl_currency_history_price_task(self):
        """
        获取所有的货币的历史价格
        :return:
        """
        currency_cursor = crawl_db[CrawlCollections.COINMARKET_CURRENCY_PRICE].find()
        for currency in currency_cursor:
            self.crawl_currency_history_price(currency['_id'])

    @staticmethod
    def crawl_currency_total_market():
        """
        请求总市值的接口
        :param self:
        :return:
        """
        payload = {'project': 'NeoScrapy', 'spider': 'coinmarketcap', 'func': 'TOTAL_MARKET'}
        return scrapy_add_job(payload).json()

    @staticmethod
    def crawl_currency_detail(with_logo=False):
        """
        通过网页爬取currency的基本信息，如网址，reddit地址的等信息
        :param self:
        :param with_logo: true则会爬取currency的logo图片
        :return:
        """
        payload = {'project': 'NeoScrapy', 'spider': 'coinmarketcap', 'func': 'DETAIL', 'with_logo': with_logo}
        return scrapy_add_job(payload).json()

    @staticmethod
    def crawl_currency_history_price(currency_id):
        """
        根据currency_id获取currency的历史价格信息
        :param self:
        :param currency_id: 货币id
        :return:
        """
        payload = {'project': 'NeoScrapy', 'spider': 'coinmarketcap', 'func': 'HISTORY_PRICE',
                   'currency_id': currency_id}
        return scrapy_add_job(payload).json()

    @staticmethod
    def cancel_coinmarket_jobs():
        res_json = scrapy_list_job({'project': 'NeoScrapy', 'spider': 'coinmarketcap'}).json()
        if res_json['status'] == 'ok':
            pending_jobs = res_json['pending']
            for job in pending_jobs:
                job_id = job['id']
                print(scrapy_cancel_job({'project': 'NeoScrapy', 'job': job_id}))

    @staticmethod
    def crawl_currency_price():
        url = 'https://api.coinmarketcap.com/v1/ticker/?limit=0'
        print('get currency prices')
        res = requests.get(url)
        currencies = res.json()
        if currencies is not None:
            for item in currencies:
                item['rank'] = str_to_int(item['rank'])
                item['price_usd'] = str_to_float(item['price_usd'])
                item['price_btc'] = str_to_float(item['price_btc'])
                item['24h_volume_usd'] = str_to_float(item['24h_volume_usd'])
                item['market_cap_usd'] = str_to_float(item['market_cap_usd'])
                item['available_supply'] = str_to_float(item['available_supply'])
                item['total_supply'] = str_to_float(item['total_supply'])
                item['max_supply'] = str_to_float(item['max_supply'])
                item['percent_change_1h'] = str_to_float(item['percent_change_1h'])
                item['percent_change_24h'] = str_to_float(item['percent_change_24h'])
                item['percent_change_7d'] = str_to_float(item['percent_change_7d'])
                item['last_updated'] = str_to_float(item['last_updated'])
                crawl_db[CrawlCollections.COINMARKET_CURRENCY_PRICE].find_one_and_update({'_id': item['id']},
                                                                                         {'$set': item},
                                                                                         upsert=True)
