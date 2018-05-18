from pymongo import MongoClient, ASCENDING, DESCENDING
from api.config import Conf


client = Conf.CLIENT
crawl_db = client[Conf.CRAWL_DATABASE]
stat_db = client[Conf.STAT_DATABASE]
relation_db = client[Conf.RELATION_DATABASE]
app_db = client[Conf.APP_DATABASE]


class CrawlCollections:
    # config options
    APP_CONFIG = '_app_config_'

    # bitcointalk collections
    BTT_LINK = 'btt_link'                                       # announcement 下的所有帖子
    BTT_COMMENT = 'btt_comment'                                 # 帖子下的评论
    BTT_USER = 'btt_user'                                       # 用户的基本信息
    BTT_USER_STAT = 'btt_stat'                                  # 用户的统计信息，来自https://bitcointalk.org/index.php?action=profile;u={0};sa=statPanel的信息
    BTT_USER_HISTORY_POST = 'btt_history_post'                  # 用户历史评论数据
    BTT_USER_HISTORY_START = 'btt_history_start'                # 用户历史发帖信息

    # github collections
    GIT_COMMIT = 'git_commit'                                   # github commit 数据
    GIT_CONTRIBUTOR = 'git_contributor'                         # 对仓库产生贡献者的用户信息
    GIT_PROJECT = 'git_project'                                 # 项目信息
    # ....
    # github api 提供的数据统计接口
    # type：scf [stat code frequency] | sc [stats contributors] | sca [stat comment activity]
    GIT_STAT = 'git_stat'

    # reddit collections
    REDDIT_LINK = 'reddit_link'                                 # reddit中的发帖信息
    REDDIT_COMMENT = 'reddit_comment'                           # reddit中的评论信息
    REDDIT_SUBREDDIT = 'reddit_subreddit'                       # subreddit基本信息
    REDDIT_USER = 'reddit_user_basic_info'                      # reddit用户基本信息
    REDDIT_HISTORY_COMMENT = 'reddit_history_comment'           # 用户评论的历史信息
    REDDIT_HISTORY_SUBMIT = 'reddit_history_submitted'          # 用户提交(发帖)的历史信息

    # tokenmarket currencies
    TOKENMARKET_CURRENCIES = 'tokenmarket_currency'             # tokenmarket中货币的信息

    # coinmarket currencies
    COINMARKET_CURRENCIES = 'coinmarket_currency'               # coinmarket中货币信息
    COINMARKET_CURRENCY_PRICE = 'coinmarket_currency_price'     # coinmarket货币价格信息
    COINMARKET_HISTORY_PRICE =  'coinmarket_history_currency'   # coinmarket中历史价格信息
    COINMARKET_TOTAL_MARKET_CAP = 'coinmarket_total_market_cap' # coinmarket中历史市场cap数据
    COINDESK_NEWS = 'coindesk_news'                             # coindesk 中的资讯信息
    ICOHOLDER_BASE = 'icoholder_base'                           # 从ico holder中获取的货币ico筹集的时间和总金额
    ICOHOLDER_MYTOKEN = 'icoholder_mytoken'                     # 从mytoken上获取ico时间和价格


class RelationCollections:
    # relation collections
    RELATION_CURRENCY_REDDIT = 'relation_currency_reddit'       # 保存reddit和currency之间的对应关系
    RELAITON_CURRENCY_GITHUB = 'relation_currency_github'       # 保存gitbub和currency之间的对应关系
    RELATION_CURRENCY_BTT = 'relation_currency_btt'             # 保存bitcointalk和currency之间的对应关系
    RELATION_CURRENCY_ANN = 'relation_currency_ann'             # 保存bitcointalk中currency的ANN贴
    RELATION_CURRENCY_TAG = 'relation_currency_tag'             # 保存currency和标签的对应关系


class StatCollections:
    BTT_LINK_STAT = 'btt_link_stat'                             # bitcointalk中单个帖子回复信息的统计
    BTT_USER_RELATION_STAT = 'btt_user_relation_stat'           # bitcointalk中单个贴中的用户回帖关系图
    BTT_LINK_TIME_DISTRIBUTE = 'btt_link_time_distribute'       # bitcointalk中单个帖子随时间的分布图
    BTT_COMMENT_KEYWORD_STAT = 'btt_keyword_stat'               # bitcointalk中keyword的用户分布统计
    BTT_USER_HISTORY_STAT = 'btt_user_history_stat'             # bitcointalk中用户历史足迹分布
    BTT_KOL = 'btt_kol'                                         # bitcointalk中的kol用户
    REDDIT_KEY_USER = 'reddit_key_user'                         # reddit中每个subreddit中的keyuser信息
    REDDIT_KOL_HISTORY = 'reddit_kol_history'
    REDDIT_CURRENCY_RELATED_KEY_WORD_STAT = 'reddit_currency_related_keyword_stat' # 项目关联关键词分析结果
    SUBREDDIT_TIME_DISTRIBUTE = 'subreddit_time_distribute'     # reddit中发帖和评论的时间分布
    HISTORY_CURRENCY_PRICE_STAT = 'history_currency_price_stat' # 保存分析后的历史货币价格信息
    HISTORY_CURRENCY_RANK = 'history_currency_rank'             # 货币历史排名变化信息


class AppCollections:
    CURRENCY_TAGS = 'currency_tags'                             # 货币标签
    CURRENCY_RELATED_KEY_WORD = 'currency_related_key_word'     # 项目关联的关键词
