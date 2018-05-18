from api.common.crawl.scrapyd import scrapy_add_job, scrapy_list_job, scrapy_cancel_job


class CoindeskNewsCrawl:

    @staticmethod
    def crawl_coindesk_news():
        payload = {'project': 'NeoScrapy', 'spider': 'coindesk'}
        return scrapy_add_job(payload).json()


if __name__ == '__main__':
    CoindeskNewsCrawl.crawl_coindesk_news()
