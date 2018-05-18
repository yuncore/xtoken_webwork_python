from api.common.crawl.scrapyd import scrapy_add_job, scrapy_list_job, scrapy_cancel_job
from api.db import crawl_db, CrawlCollections, relation_db, RelationCollections


class GithubCrawl:

    def crawl_project_task(self, with_user=False):
        relation_cursor = relation_db[RelationCollections.RELAITON_CURRENCY_GITHUB].find()
        for r in relation_cursor:
            self.crawl_project(r['full_name'], with_user)

    @staticmethod
    def crawl_project(full_name, with_user=False):
        """
        根据GitHub project的full name获取项目的信息
        :param full_name:
        :param with_user: 是否爬取该项目的贡献者信息，由于请求该接口花费时间较长，并且实时性要求不高。可以间隔较长的时间请求一次。
        :return:
        """
        payload = {'project': 'NeoScrapy', 'spider': 'github', 'func': 'PROJECT', 'full_name': full_name,
                   'with_user': with_user}
        return scrapy_add_job(payload).json()

    @staticmethod
    def cancel_github_jobs():
        res_json = scrapy_list_job({'project': 'NeoScrapy', 'spider': 'github'}).json()
        if res_json['status'] == 'ok':
            pending_jobs = res_json['pending']
            for job in pending_jobs:
                job_id = job['id']
                scrapy_cancel_job({'project': 'NeoScrapy', 'job': job_id})
