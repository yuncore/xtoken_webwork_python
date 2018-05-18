from api.db import stat_db, crawl_db, CrawlCollections, StatCollections, relation_db, RelationCollections, DESCENDING, \
    ASCENDING
from datetime import datetime
from api.common.utils.util import timestamp_to_datetime
import time
import re
import json
import pandas as pd
import numpy as np


# 将从coinmarketcap中抓取下来的货币历史价格信息重新加工，生成价格排名
def history_price_time_resample():
    """
    遍历获取到的历史价格数据的货币，整理对应价格的时间戳数据，使之对应为%Y:%m:%s 00:00:00。即将时分秒设置为0.
    将整理后的数据存入stat DB中的history_currency_price集合中。该集合的基本结构为：
    {
        "_id": 1510675200.0,
        "market_cap": {
            'bitcoin': 12345678,
            ...
        },
        "price_btc": {
            'bitcoin': 1,
            ....
        },
        "price_usd": {
            'bitcoin': 12345678,
        },
        "market_cap_total": 12345678,
        "volume_usd_total": 12345678,
        "market_cap_altcoin": 12345678,
        "volume_usd_altcoin": 1234567
    }
    :return:
    """
    # 遍历所有货币的历史数据，逐个进行数据整理, 取消cursor的超时时间，需要显式关闭cursor
    currency_cursor = crawl_db[CrawlCollections.COINMARKET_HISTORY_PRICE].find(no_cursor_timeout=True)
    currencies = [{'_id': currency['_id'], 'data': currency['data']} for currency in currency_cursor]

    for currency in currencies:
        start = time.time()
        currency_id = currency['_id']
        try:
            data = json.loads(currency['data'])
        except Exception:
            print('error occurred when convert currency data to json')
            continue
        market_cap = data['market_cap_by_available_supply']
        price_usd = data['price_usd']
        price_btc = data['price_btc']
        price_time_resample('market_cap', market_cap, currency_id)
        price_time_resample('price_usd', price_usd, currency_id)
        price_time_resample('price_btc', price_btc, currency_id)
        print('resample {0} completed, used {1}'.format(currency_id, time.time() - start))
    currency_cursor.close()

    # 整理market_cap_total和market_cap_altcoin
    market_cap_total = crawl_db[CrawlCollections.COINMARKET_TOTAL_MARKET_CAP].find_one({'type': 'MARKETCAP_TOTAL'})

    # 保存market cap total数据
    if market_cap_total is not None:
        try:
            json_data = json.loads(market_cap_total['data'])
            market_cap_data = json_data['market_cap_by_available_supply']
            volume_usd_data = json_data['volume_usd']
            price_time_resample('market_cap_total', market_cap_data)
            price_time_resample('volume_usd_total', volume_usd_data)
        except Exception:
            print('error occurred when convert market cap or volume usd to json')

    market_cap_altcoin = crawl_db[CrawlCollections.COINMARKET_TOTAL_MARKET_CAP].find_one({'type': 'MARKETCAP_ALTCOIN'})

    # 保存market cap altcoin数据
    if market_cap_altcoin is not None:
        try:
            json_data = json.loads(market_cap_altcoin['data'])
            market_cap_data = json_data['market_cap_by_available_supply']
            volume_usd_data = json_data['volume_usd']
            price_time_resample('market_cap_altcoin', market_cap_data)
            price_time_resample('volume_usd_altcoin', volume_usd_data)
        except Exception:
            print('error occurred when convert market cap or volume usd to json')


def price_time_resample(field_name, data, currency_id=None):
    """
    整理market cap的时间戳，并数据更新到数据库中
    :param field_name: 字段名称
    :param data: market_cap / price_usd / price_btc价格数据
    :param currency_id: 货币id
    :return:
    """
    # 从coinmarketcap中抓取的价格数据是按照时间顺序排列的，反序遍历货币价格数据
    # 为了节约计算时间，在设置了start的情况下，如果价格数据的时间小于start则跳出循环
    start = time.time() - 3600 * 24 * 7
    

    for item in reversed(data):
        original_date = datetime.fromtimestamp(item[0] / 1000)

        # 将数据转化成格式为%Y-%m-%d字符串，再将字符串转换为时间戳，以达到将时分秒置为0的目的
        str_time = original_date.strftime('%Y-%m-%d')
        format_date = datetime.strptime(str_time, '%Y-%m-%d').timestamp()

        # 如果当前计算的时间小于分析的起识时间，跳出循环
        if start and format_date < start:
            break

        # 在stat collection中查找该日期的记录
        price_data = stat_db[StatCollections.HISTORY_CURRENCY_PRICE_STAT].find_one({'_id': format_date})

        # 如果集合名字是'market_cap', 'price_usd', 'price_btc'其中的一个，插入和更新的集合名字写法不一样
        if field_name in ['market_cap', 'price_usd', 'price_btc']:
            if price_data is not None:
                stat_db[StatCollections.HISTORY_CURRENCY_PRICE_STAT].update_one(
                    {'_id': price_data['_id']},
                    {'$set': {'{0}.{1}'.format(field_name, currency_id): item[1]}})
            else:
                stat_db[StatCollections.HISTORY_CURRENCY_PRICE_STAT].insert_one(
                    {'_id': format_date, field_name: {'{0}'.format(currency_id): item[1]}})
        else:
            # 集合名字是market_cap_total，volume_usd_total, market_cap_altcoin, volume_usd_altcoin
            if price_data is not None:
                stat_db[StatCollections.HISTORY_CURRENCY_PRICE_STAT].update_one({'_id': price_data['_id']},
                                                                                {'$set': {field_name: item[1]}})
            else:
                stat_db[StatCollections.HISTORY_CURRENCY_PRICE_STAT].insert_one(
                    {'_id': format_date, field_name: item[1]})


def currency_rank_analyze():
    """计算每个货币的排名随日期的变化"""
    try:
        d = stat_db[StatCollections.HISTORY_CURRENCY_RANK].find_one({'currency_id': 'bitcoin'})
        latest = max(d['rank_market_cap'], key=lambda x: x[0])
    except (TypeError, KeyError) as e:
        latest = [0]
    history_data = stat_db[StatCollections.HISTORY_CURRENCY_PRICE_STAT].find(
        {'_id': {'$gt': latest[0]}}, no_cursor_timeout=True).sort([('_id', ASCENDING)])
    i = 0
    print(latest)
    for data in history_data:
        t = data['_id']
        i = i + 1
        if i % 100 == 0:
            print(t, i)

        # 现在不需要使用 price_usd计价
        # prices = data['price_usd']
        # sort_price = sorted([[k, prices[k]] for k in list(prices.keys())], key=lambda x: x[1], reverse=True)
        # for index, item in enumerate(sort_price):
        #     stat_db[StatCollections.HISTORY_CURRENCY_RANK].find_one_and_update(
        #         {'currency_id': item[0]},
        #         {'$push': {'rank_price': [t, index, len(sort_price)]}},
        #         upsert=True)

        market_caps = data['market_cap']
        sort_market_cap = sorted([[k, market_caps[k]] for k in list(market_caps.keys())], key=lambda x: x[1], reverse=True)
        for index, item in enumerate(sort_market_cap):
            stat_db[StatCollections.HISTORY_CURRENCY_RANK].find_one_and_update(
                {'currency_id': item[0]},
                {'$push': {'rank_market_cap': [t, index, len(sort_market_cap)]}},
                upsert=True)


def currency_rank_analyze2(ahead_time=0):
    """计算每个货币排名随时间的变化"""
    currencies = crawl_db[CrawlCollections.COINMARKET_CURRENCY_PRICE].find({'_id': '300-token'})
    currency_ids = [c['id'] for c in currencies]
    start = time.time()
    for c_id in currency_ids:
        currency_rank = stat_db[StatCollections.HISTORY_CURRENCY_RANK].find_one({'currency_id': c_id})
        if currency_rank:
            market_cap_rank = currency_rank['rank_market_cap']
            sorted_rank = sorted(market_cap_rank, key=lambda x: x[0], reverse=True)
            latest_time = sorted_rank[0][0]
        else:
            latest_time = 0
            market_cap_rank = []
        print('latest', timestamp_to_datetime(latest_time))
        if latest_time:
            latest_time = latest_time - ahead_time
        histories = stat_db[StatCollections.HISTORY_CURRENCY_PRICE_STAT].find(
            {'_id': {'$gt': latest_time}}).sort([('_id', ASCENDING)])
        for history_data in histories:
            market_caps = history_data['market_cap']
            t = history_data['_id']
            for index, item in enumerate(market_cap_rank):
                if item[0] == t:
                    del market_cap_rank[index]
            sorted_market_cap = sorted([[k, market_caps[k]] for k in market_caps.keys()], key=lambda x: x[1], reverse=True)
            for index, item in enumerate(sorted_market_cap):
                if item[0] == c_id:
                    market_cap_rank.append([t, index, len(sorted_market_cap)])
                    print('append', [timestamp_to_datetime(t), index, len(sorted_market_cap)])
                    break
            stat_db[StatCollections.HISTORY_CURRENCY_RANK].find_one_and_update(
                {'currency_id': c_id},
                {'$set': {"rank_market_cap": market_cap_rank}},
                upsert=True)
        print('analyze {0} completed, used {1}'.format(c_id, time.time() - start))


def check_history_rank_data(c_id):
    cs = stat_db[StatCollections.HISTORY_CURRENCY_RANK].find({'currency_id': c_id})
    for item in cs:
        rank = item['rank_market_cap']
        check_rank(rank)
        new_rank = []
        times = []
        for r in rank:
            if r[0] not in times:
                new_rank.append(r)
                times.append(r[0])
        print('after....')
        check_rank(new_rank)
        stat_db[StatCollections.HISTORY_CURRENCY_RANK].find_one_and_update(
            {'_id': item['_id']},
            {'$set': {'rank_market_cap': new_rank}})


def check_rank(rank):
    for index1, item1 in enumerate(rank):
        count = 0
        if item1[1] == 0:
            print('error data', item1)
        for index2, item2 in enumerate(rank):
            if item1[0] == item2[0]:
                count = count + 1
        if count > 1:
            print('dump data {0}---{1}'.format(count, item1[0]))


if __name__ == '__main__':
    # currency_rank_analyze2(ahead_time=24 * 3600 * 30)
    # check_history_rank_data('300-token')
    history_price_time_resample()
