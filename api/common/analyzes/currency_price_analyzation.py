from api.db import stat_db, crawl_db, CrawlCollections, StatCollections, relation_db, RelationCollections, DESCENDING, \
    ASCENDING
from datetime import datetime
from api.common.utils.util import timestamp_to_datetime, str_to_timestamp
import pandas as pd
import time
import json


# 将从coinmarketcap中抓取下来的货币历史价格信息重新加工，生成价格排名
def history_price_time_resample(ahead_time=0):
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
    currency_cursor = crawl_db[CrawlCollections.COINMARKET_HISTORY_PRICE].find({'_id': 'jobscoin'}, no_cursor_timeout=True)
    for currency in currency_cursor:
        start = time.time()
        currency_id = currency['_id']
        print('start analyze {0}'.format(currency_id))
        # 查询上一次数据分析的时间，如果数据库中纪录不存在，则将其设置为0
        if 'last_resample_time' in currency:
            last_resample_time = currency['last_resample_time']
        else:
            last_resample_time = 0

        # 将json转换为dict
        try:
            price_data = json.loads(currency['data'])
        except Exception:
            print('error occurred when convert currency data to json')
            continue

        # 分析market_cap, price_usd, price_btc的数据，对应的日期数据
        market_cap = filter_not_resample_data(price_data['market_cap_by_available_supply'], last_resample_time, ahead_time)
        # print('market_cap length {0}'.format(len(market_cap)))
        for item in market_cap:
            # format_date = reformat_timestamp(item[0])
            stat_db[StatCollections.HISTORY_CURRENCY_PRICE_STAT].find_one_and_update(
                {'_id': item[0]},
                {'$set': {'{0}.{1}'.format('market_cap', currency_id): item[1]}}, upsert=True)

        price_usd = filter_not_resample_data(price_data['price_usd'], last_resample_time, ahead_time)
        for item in price_usd:
            # format_date = reformat_timestamp(item[0])
            stat_db[StatCollections.HISTORY_CURRENCY_PRICE_STAT].find_one_and_update(
                {'_id': item[0]},
                {'$set': {'{0}.{1}'.format('price_usd', currency_id): item[1]}}, upsert=True)

        price_btc = filter_not_resample_data(price_data['price_btc'], last_resample_time, ahead_time)
        for item in price_btc:
            # format_date = reformat_timestamp(item[0])
            stat_db[StatCollections.HISTORY_CURRENCY_PRICE_STAT].find_one_and_update(
                {'_id': item[0]},
                {'$set': {'{0}.{1}'.format('price_btc', currency_id): item[1]}}, upsert=True)

        # 设置新的resample time
        try:
            new_resample_time = price_data['market_cap_by_available_supply'][-1][0]
        except IndexError:
            new_resample_time = 0
        crawl_db[CrawlCollections.COINMARKET_HISTORY_PRICE].find_one_and_update(
            {'_id': currency_id},
            {'$set': {'last_resample_time': new_resample_time}})

        print('resample {0} completed, used {1}'.format(currency_id, time.time() - start))

    # 关闭数据库连接
    currency_cursor.close()


def market_cap_total_resample(ahead_time=0):
    # 整理market_cap_total和market_cap_altcoin
    market_cap_total = crawl_db[CrawlCollections.COINMARKET_TOTAL_MARKET_CAP].find_one({'type': 'MARKETCAP_TOTAL'})

    # 处理market cap total数据
    if market_cap_total is not None:
        json_data = json.loads(market_cap_total['data'])
        market_cap = json_data['market_cap_by_available_supply']
        volume_usd = json_data['volume_usd']
        if 'last_resample_time' in market_cap_total:
            last_resample_time = market_cap_total['last_resample_time']
        else:
            last_resample_time = 0

        # 分析market_cap对应的日期数据
        market_cap_resample = filter_not_resample_data(market_cap, last_resample_time, ahead_time)
        for item in market_cap_resample:
            stat_db[StatCollections.HISTORY_CURRENCY_PRICE_STAT].find_one_and_update(
                {'_id': item[0]},
                {'$set': {'market_cap_total': item[1]}}, upsert=True)

        # 分析volume usd 对应的日期数据
        volume_usd_resample = filter_not_resample_data(volume_usd, last_resample_time, ahead_time)
        for item in volume_usd_resample:
            stat_db[StatCollections.HISTORY_CURRENCY_PRICE_STAT].find_one_and_update(
                {'_id': item[0]},
                {'$set': {'volume_usd_total': item[1]}}, upsert=True)

        # 设置新的resample time
        try:
            new_resample_time = market_cap[-1][0]
        except IndexError:
            new_resample_time = 0
        crawl_db[CrawlCollections.COINMARKET_TOTAL_MARKET_CAP].find_one_and_update(
            {'type': 'MARKETCAP_TOTAL'},
            {'$set': {'last_resample_time': new_resample_time}})


def market_cap_altcoin_resample(ahead_time=0):
    market_cap_altcoin = crawl_db[CrawlCollections.COINMARKET_TOTAL_MARKET_CAP].find_one({'type': 'MARKETCAP_ALTCOIN'})

    # 保存market cap altcoin数据
    if market_cap_altcoin is not None:
        json_data = json.loads(market_cap_altcoin['data'])
        market_cap = json_data['market_cap_by_available_supply']
        volume_usd = json_data['volume_usd']
        if 'last_resample_time' in market_cap_altcoin:
            last_resample_time = market_cap_altcoin['last_resample_time']
        else:
            last_resample_time = 0

        # 分析market_cap对应的日期数据
        market_cap_resample = filter_not_resample_data(market_cap, last_resample_time, ahead_time)
        for item in market_cap_resample:
            stat_db[StatCollections.HISTORY_CURRENCY_PRICE_STAT].find_one_and_update(
                {'_id': item[0]},
                {'$set': {'market_cap_altcoin': item[1]}}, upsert=True)

        # 分析volume usd 对应的日期数据
        volume_usd_resample = filter_not_resample_data(volume_usd, last_resample_time, ahead_time)
        for item in volume_usd_resample:
            stat_db[StatCollections.HISTORY_CURRENCY_PRICE_STAT].find_one_and_update(
                {'_id': item[0]},
                {'$set': {'volume_usd_altcoin': item[1]}}, upsert=True)

        # 设置新的resample time
        try:
            new_resample_time = market_cap[-1][0]
        except IndexError:
            new_resample_time = 0
        crawl_db[CrawlCollections.COINMARKET_TOTAL_MARKET_CAP].find_one_and_update(
            {'type': 'MARKETCAP_ALTCOIN'},
            {'$set': {'last_resample_time': new_resample_time}}
        )


def filter_not_resample_data(data, last_resample_time, ahead_time=0):
    """
    :param data: 需要过滤的数组数据
    :param last_resample_time: 上一次数据清洗的时间， 单位是毫秒
    :param ahead_time: 在上一次数据清洗时间的基础上提前的时间，单位是毫秒
    :return:
    """
    original_data = []
    if last_resample_time == 0:
        t = time.time() * 1000 - ahead_time
    else:
        t = last_resample_time
    for item in reversed(data):
        if item[0] > t:
            original_data.append(item)
        else:
            break
    if len(original_data) > 0:
        df = pd.DataFrame(original_data, columns=['time', 'price'])
        df.time = df['time'].apply(lambda x: datetime.fromtimestamp(x / 1000))
        df.set_index('time', inplace=True)
        df = df.resample('D').mean()
        df = df.fillna(0)
        return [[index.timestamp(), float(row['price'])] for index, row in df.iterrows()]
    else:
        return []


def currency_rank_analyze(ahead_time=0):
    """计算每个货币排名随时间的变化
    :param ahead_time 单位精确到秒
    """
    currencies = crawl_db[CrawlCollections.COINMARKET_CURRENCY_PRICE].find({})
    currency_ids = [c['id'] for c in currencies]
    start = time.time()
    for c_id in currency_ids:
        currency_rank = stat_db[StatCollections.HISTORY_CURRENCY_RANK].find_one({'currency_id': c_id})

        # 计算最新分析的时间latest_time 精确到秒
        if currency_rank:
            market_cap_rank = currency_rank['rank_market_cap']
            sorted_rank = sorted(market_cap_rank, key=lambda x: x[0], reverse=True)
            try:
                latest_time = sorted_rank[0][0]
            except IndexError:
                market_cap_rank = []
                latest_time = 0
                print(sorted_rank)
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

            # 防止重复添加数据
            for index, item in enumerate(market_cap_rank):
                if item[0] == t:
                    del market_cap_rank[index]

            # 将当天的市值排序
            if c_id in market_caps:
                df = pd.DataFrame([[k, market_caps[k]] for k in market_caps], columns=['name', 'values'])
                df = df.fillna(0)
                df = df.sort_values(by='values', ascending=False)
                count = len(df.index)
                df['sort'] = range(count)
                rank = int(df[df['name'] == c_id].iloc[0, 2])
                market_cap_rank.append([t, rank, count])
                # print('append data {0} {1} {2}'.format(timestamp_to_datetime(t), rank, count))

        stat_db[StatCollections.HISTORY_CURRENCY_RANK].find_one_and_update(
            {'currency_id': c_id},
            {'$set': {"rank_market_cap": market_cap_rank}},
            upsert=True)
        print('analyze {0} completed, used {1}'.format(c_id, time.time() - start))


if __name__ == '__main__':
    history_price_time_resample(ahead_time=24 * 3600 * 50 * 1000)
    market_cap_total_resample(ahead_time=24 * 3600 * 50 * 1000)
    market_cap_altcoin_resample(ahead_time=24 * 3600 * 50 * 1000)
    currency_rank_analyze(ahead_time=24 * 3600 * 50)

