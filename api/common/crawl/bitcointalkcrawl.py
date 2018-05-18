from api.common.crawl.scrapyd import scrapy_add_job, scrapy_list_job, scrapy_cancel_job
from api.db import crawl_db, CrawlCollections, relation_db, RelationCollections


class BTTCrawl:

    def crawl_ann_link_comment_task(self):
        """
        遍历relation_currency_ann，获取和currency相关的topic_id。
        将topic_id加入爬虫任务队列，依次爬取每个链接下的评论。
        :return:
        """
        ann_list = relation_db[RelationCollections.RELATION_CURRENCY_ANN].find()
        for link in ann_list:
            print(self.crawl_ann_link_comment(link['topic_id']))

    @staticmethod
    def crawl_ann_link_comment(link_id, force=False):
        """
        获取单个bitcointalk中帖子下的所有评论
        :param self:
        :param link_id: bitcointalk中的帖子的topic_id ,对应payload中的ids *ids不是数组。
        :param force: 是否强制获取所有时间内的回复，默认为False，即在获取评论的时候会和数据库中已有的最新回复时间做对比，只拿最新的回复
        :return:
        """
        payload = {'project': 'NeoScrapy', 'spider': 'bitcointalk', 'ids': link_id, 'func': 'COMMENT', 'force': force}
        return scrapy_add_job(payload).json()

    @staticmethod
    def crawl_user_info(user_id):
        """
        获取用户的基本信息、统计信息
        :param user_id: 用户的id
        :return:
        """
        payload = {'project': 'NeoScrapy', 'spider': 'bitcointalk', 'ids': user_id, 'func': 'USER'}
        return scrapy_add_job(payload).json()

    @staticmethod
    def crawl_links():
        payload = {'project': 'NeoScrapy', 'spider': 'bitcointalk', 'func': 'ANNLINK'}
        return scrapy_add_job(payload).json()

    @staticmethod
    def cancel_btt_jobs():
        res_json = scrapy_list_job({'project': 'NeoScrapy', 'spider': 'bitcointalk'}).json()
        if res_json['status'] == 'ok':
            pending_jobs = res_json['pending']
            for job in pending_jobs:
                job_id = job['id']
                scrapy_cancel_job({'project': 'NeoScrapy', 'job': job_id})
