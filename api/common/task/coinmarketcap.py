from api.common.analyzes.currency_price_analyzation import history_price_time_resample, currency_rank_analyze,\
    market_cap_total_resample, market_cap_altcoin_resample
from api.common.crawl.coinmarketcrawl import CoinMarketCrawl


coin_market_crawl = CoinMarketCrawl()


def coinmarketcap_crawl_tasks():
    print('crawl coinmarketcap data')
    # 定时爬取货币历史价格
    coin_market_crawl.crawl_currency_history_price_task()
    # 定时爬取总市值
    coin_market_crawl.crawl_currency_total_market()
    # 定时爬取货币的详细信息
    coin_market_crawl.crawl_currency_detail()


def coinmarketcap_analyze_tasks():
    print('analyze coinmarketcap data')
    history_price_time_resample()
    currency_rank_analyze()
    market_cap_total_resample()
    market_cap_altcoin_resample()

if __name__ == '__main__':
    coinmarketcap_analyze_tasks()