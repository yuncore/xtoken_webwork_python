from api.common.analyzes.reddit_analyzation import analyze_kol_batch_entry
from api.common.crawl.redditcrawl import RedditCrawl


reddit_crawl = RedditCrawl()


def reddit_crawl_tasks():
    print('crawl reddit data')
    # 定时爬取subreddit相关的信息
    reddit_crawl.crawl_reddit_subreddit_task()
    # 定时爬取reddit中所有subreddit的帖子
    reddit_crawl.crawl_reddit_link_task()
    # 定时爬取所有subreddit下帖子的回复
    reddit_crawl.crawl_reddit_comment_task()


def reddit_analyze_tasks():
    print('analyze reddit data')
    # 定时分析reddit里面的关键人物KOL，评论KOL和发帖KOL
    analyze_kol_batch_entry()


if __name__ == '__main__':
    reddit_crawl.crawl_reddit_subreddit_task()