from api.common.analyzes.bitcointalk_analyzation import link_stat_batch, user_history_stat
from api.common.crawl.bitcointalkcrawl import BTTCrawl


btt_crawl = BTTCrawl()


def bitcointalk_crawl_tasks():
    print('crawl bitcointalk data')
    # 定时爬取bitcointalk中的帖子
    btt_crawl.crawl_links()
    # 定时爬取bitcointalk中的帖子下的评论
    btt_crawl.crawl_ann_link_comment_task()


def bitcointalk_analyze_tasks():
    print('analyze bitcointalk data')
    # 定时分析抓取到的数据
    # 分析用户的评论生成用户互相间引用关系图
    link_stat_batch('quote_graph')
    # 分析单个btt帖子时间的日回复数量
    link_stat_batch('comment_time_distribute')
    # 单个用户历史评论足迹分布
    user_history_stat()