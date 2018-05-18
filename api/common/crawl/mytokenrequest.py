import hashlib
from datetime import datetime
import time
import requests
from api.db import crawl_db, relation_db, app_db, AppCollections, CrawlCollections, RelationCollections, \
    stat_db, StatCollections, ASCENDING, DESCENDING
from api.config import Conf


def generateMyTokenMD5(timestamp):
    str = 'thalesky_eos_' + timestamp
    h1 = hashlib.md5()
    h1.update(str.encode(encoding='utf-8'))
    return h1.hexdigest()


def requestMyTokenMsg(symbol):
    """
    向mytoken发送数据请求获取ioc的时间和价格
    :param symbol: 货币的简称
    :return:
    """
    com_id = symbol + '_cny'
    device_model = 'MI MAX'
    device_os = '6.0.1'
    device_token = 'AuZOU902DNVPYWIzy6oNSRsNcUljlqC5P6MNF-5wc5YQ'
    language = 'zh_CN'
    legal_currency = 'CNY'
    market_id = '1303'
    market_name = 'cmc'
    mytoken = 'f5c860eb5921fe098ffd224f21cf1f56'
    platform = 'android'
    udid = 'ffffffff-dcee-ce16-ffff-ffffaa541fb7'
    v = '1.8.0'

    timenow = time.time()
    timestamp = (int(timenow))
    code = generateMyTokenMD5(str(timestamp))

    params = {
        "timestamp": timestamp,
        "code": code,
        "com_id": com_id,
        "device_model": device_model,
        "device_os": device_os,
        "device_token": device_token,
        "language": language,
        "legal_currency": legal_currency,
        "market_id": market_id,
        "market_name": market_name,
        "mytoken": mytoken,
        "platform": platform,
        "udid": udid,
        "v": v
    }
    URL = 'http://api.lb.mytoken.org/currency/currencydetail'
    rsMsg = requests.get(URL, params=params).json()

    return rsMsg


def getAllCurrencyList():
    page_num = 20
    total = crawl_db[CrawlCollections.COINMARKET_CURRENCIES].count()
    page = 0
    if total % page_num == 0:
        page = int(total / page_num)
    else:
        page = int(total / page_num) + 1
    count = 0
    for x in range(page):
        items = crawl_db[CrawlCollections.COINMARKET_CURRENCIES].find({}, {'data.symbol': 1}) \
            .skip(x * page_num).limit(page_num).sort([('data.sort', 1)])

        for item in items:
            symbol = item['data']['symbol']
            try:
                msg = requestMyTokenMsg(symbol)
                data = msg['data']
                exchange_rate_display = ''
                ico_date_display = ''
                raised_amount_display = ''
                if 'exchange_rate_display' in data:
                    exchange_rate_display = data['exchange_rate_display']
                if 'ico_date_display' in data:
                    ico_date_display = data['ico_date_display']
                if 'raised_amount_display' in data:
                    raised_amount_display = data['raised_amount_display']
                sdata = {
                    'symbol': symbol,
                    'exchange_rate_display': exchange_rate_display,
                    'ico_date_display': ico_date_display,
                    'raised_amount_display': raised_amount_display
                }
                # data, {'$set': {'symbol': symbol}}
                crawl_db[CrawlCollections.ICOHOLDER_MYTOKEN].update({'symbol': symbol}, sdata, upsert=True)
                time.sleep(1)
                count += 1
                print(count)
            except Exception as ex:
                print(symbol)
                print(ex)


if __name__ == '__main__':
    getAllCurrencyList()
