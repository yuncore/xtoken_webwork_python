from api.common.crawl.scrapyd import scrapy_add_job, scrapy_list_job, scrapy_cancel_job
from api.db import crawl_db, CrawlCollections, relation_db, RelationCollections


class RedditCrawl:

    def crawl_reddit_subreddit_task(self):
        relation_cursor = relation_db[RelationCollections.RELATION_CURRENCY_REDDIT].find()
        for relation in relation_cursor:
            self.crawl_reddit_subreddit(relation['sr_name'])

    def crawl_reddit_comment_task(self, force=False):
        """
        遍历relation_currency_reddit，获取和currency相关的subreddit。
        将subreddit加入爬虫序列，依次爬取该subreddit下的所有帖子的评论。
        :return:
        """
        relation_cursor = relation_db[RelationCollections.RELATION_CURRENCY_REDDIT].find()
        for relation in relation_cursor:
            self.crawl_reddit_comment(relation['sr_name'], force)

    def crawl_reddit_link_task(self):
        """
        遍历relation_currency_reddit，获取和currency相关的subreddit name。
        将subreddit name加入爬虫序列，依次爬取该subreddit下的所有帖子。
        :return:
        """
        relation_cursor = relation_db[RelationCollections.RELATION_CURRENCY_REDDIT].find()
        for relation in relation_cursor:
            self.crawl_reddit_link(relation['sr_name'])

    @staticmethod
    def crawl_reddit_link(srname):
        """
        根据subreddit name获取该话题下的帖子信息
        :param srname:
        :return:
        """
        payload = {'project': 'NeoScrapy', 'spider': 'reddit', 'func': 'LINK', 'srname': srname}
        return scrapy_add_job(payload).json()

    @staticmethod
    def crawl_reddit_comment(srname, force=False):
        """
        根据subreddit name获取该话题最近一个月帖子的评论
        :param srname:
        :param force: force为false时默认获取最近一个月的帖子下的评论。force为true时强制获取所有时间内帖子的评论。
        :return:
        """
        payload = {'project': 'NeoScrapy', 'spider': 'reddit', 'func': 'COMMENT', 'srname': srname, 'force': force}
        return scrapy_add_job(payload).json()

    @staticmethod
    def crawl_reddit_subreddit(srname):
        """
        获取subreddit的基本信息
        :param srname:
        :return:
        """
        payload = {'project': 'NeoScrapy', 'spider': 'reddit', 'func': 'SUBREDDIT', 'srname': srname}
        return scrapy_add_job(payload).json()

    @staticmethod
    def cancel_reddit_jobs():
        res_json = scrapy_list_job({'project': 'NeoScrapy', 'spider': 'reddit'}).json()
        if res_json['status'] == 'ok':
            pending_jobs = res_json['pending']
            for job in pending_jobs:
                job_id = job['id']
                scrapy_cancel_job({'project': 'NeoScrapy', 'job': job_id})


if __name__ == '__main__':
    # RedditCrawl.crawl_reddit_link('NEO')
    rc = RedditCrawl()
    rc.crawl_reddit_subreddit_task()

