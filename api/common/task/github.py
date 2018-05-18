from api.common.crawl.githubcrawl import GithubCrawl


github_crawl = GithubCrawl()


def github_crawl_tasks():
    print('crawl github data')
    # 定时爬取github相关信息
    github_crawl.crawl_project_task()