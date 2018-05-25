from apscheduler.schedulers.background import BackgroundScheduler, BlockingScheduler
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.jobstores.mongodb import MongoDBJobStore
from api.common.task.bitcointalk import bitcointalk_analyze_tasks, bitcointalk_crawl_tasks
from api.common.task.reddit import reddit_analyze_tasks, reddit_crawl_tasks
from api.common.task.github import github_crawl_tasks
from api.common.task.coinmarketcap import coinmarketcap_analyze_tasks, coinmarketcap_crawl_tasks
from api.common.crawl.coinmarketcrawl import CoinMarketCrawl
from api.config import Conf


jobstores = {
    'mongo': MongoDBJobStore(client=Conf.CLIENT),
}
executors = {
    'default': ThreadPoolExecutor(20),
    'processpool': ProcessPoolExecutor(5)
}
job_defaults = {
    'coalesce': False,
    'max_instances': 3
}

jobSchema = [
    # {'id': 'bitcointalk_analyze_tasks', 'func': bitcointalk_analyze_tasks, 'period': 7},
    # {'id': 'bitcointalk_crawl_tasks', 'func': bitcointalk_crawl_tasks, 'period': 2},
    # {'id': 'reddit_analyze_tasks', 'func': reddit_analyze_tasks, 'period': 7},
    # {'id': 'reddit_crawl_tasks', 'func': reddit_crawl_tasks, 'period': 2},
    # {'id': 'github_crawl_tasks', 'func': github_crawl_tasks, 'period': 7},
    {'id': 'coinmarketcap_crawl_tasks', 'func': coinmarketcap_crawl_tasks, 'period': 1},
    {'id': 'coinmarketcap_analyze_tasks', 'func': coinmarketcap_analyze_tasks, 'period': 1},
]


def scheduler_task():
    scheduler = BackgroundScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults)
    scheduler.add_job(CoinMarketCrawl.crawl_currency_price, 'interval', minutes=5, id='crawl_currency_price')
    for index, item in enumerate(jobSchema):
        scheduler.add_job(item['func'], 'interval', days=item['period'], id=item['id'])
    scheduler.start()

