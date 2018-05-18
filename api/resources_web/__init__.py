from flask import Blueprint
from flask_restful import Api
# bitcointalk classes
from api.resources_web.bitcointalk.link import BTTLink, BTTRelationUserLinks, BTTTimeDistributeLinks
from api.resources_web.bitcointalk.user import BTTUserHistory
from api.resources_web.bitcointalk.comment import CommentDistribute, KeyWordComment, Comment
# currency classes
from api.resources_web.currency.coinmarket_currency import CurrencySearch, CurrencyPrice, CurrencyAdditionInfo
from api.resources_web.currency.coinmarket_currency import Currency, CurrencyHistoryRank
from api.resources_web.currency.coinmarket_currency import CurrencyList
# currency tags classes
from api.resources_web.currency_tags.currency_tags import CurrencyTags, CurrencyTagRelation
# github classes
from api.resources_web.github.project import GithubProject
from api.resources_web.github.user import GitHubUser
from api.resources_web.github.stat import GithubStat
# reddit classes
from api.resources_web.reddit.user import RedditUser
from api.resources_web.reddit.user import RedditKeyUser
from api.resources_web.reddit.post import RedditPost
from api.resources_web.reddit.subreddit import RedditSubreddit
# statistics classes
from api.resources_web.statistics.statistics import KeywordList, BTTUserRelations, \
    KolUserList, HistoryCurrencyRank, HistoryTotalMarket, SubredditTimeDistribute, RedditRelatedKeyWordList, \
    RedditRelatedKeyWordAnalyzation
# coindesk news class
from api.resources_web.coindesknews.coindesknews import CoindeskNews


site_blue_print = Blueprint('site', __name__)
site_api = Api(site_blue_print)
# bitcointalk 注册路由
site_api.add_resource(BTTLink, '/bitcointalk/link','/bitcointalk/link/<link_id>')
site_api.add_resource(BTTUserHistory, '/bitcointalk/user/history')
site_api.add_resource(BTTRelationUserLinks, '/bitcointalk/user/relation/links')
site_api.add_resource(BTTTimeDistributeLinks, '/bitcointalk/time/distribute/links')
site_api.add_resource(CommentDistribute, '/bitcointalk/comment/time/distribute')
site_api.add_resource(KeyWordComment, '/bitcointalk/comment/stat/keyword')
site_api.add_resource(Comment, '/bitcointalk/comment')
# currency 注册路由
site_api.add_resource(CurrencyList, '/currency/list')
site_api.add_resource(Currency, '/currency')
site_api.add_resource(CurrencySearch, '/currency/search')
site_api.add_resource(CurrencyPrice, '/currency/price')
site_api.add_resource(CurrencyAdditionInfo, '/currency/addition/info')
site_api.add_resource(CurrencyHistoryRank, '/currency/history/rank')
# currency tags 注册路由
site_api.add_resource(CurrencyTags, '/currency/tags')
site_api.add_resource(CurrencyTagRelation, '/currency/tags/relation')
# github 注册路由
site_api.add_resource(GithubProject, '/github/project', '/github/project/<int:project_id>')
site_api.add_resource(GitHubUser, '/github/user')
site_api.add_resource(GithubStat, '/github/stat')
# reddit 注册路由
site_api.add_resource(RedditUser, '/reddit/user/<string:user_id>')
site_api.add_resource(RedditKeyUser, '/reddit/keyuser')
site_api.add_resource(RedditPost, '/reddit/post','/reddit/post/<string:post_id>')
site_api.add_resource(RedditSubreddit, '/reddit/subreddit','/reddit/subreddit/<string:sr_id>')
# statistics 注册路由
site_api.add_resource(KeywordList, '/stat/keywords')
site_api.add_resource(BTTUserRelations, '/bitcointalk/user/relation')
site_api.add_resource(KolUserList, '/bitcointalk/kol/list')
site_api.add_resource(HistoryCurrencyRank, '/stat/history/rank')
site_api.add_resource(HistoryTotalMarket, '/stat/history/total')
site_api.add_resource(SubredditTimeDistribute, '/stat/reddit/distribute')
site_api.add_resource(RedditRelatedKeyWordList, '/stat/reddit/related/keywords')
site_api.add_resource(RedditRelatedKeyWordAnalyzation, '/stat/reddit/keyword/analyze')
# coindesk news 注册路由
site_api.add_resource(CoindeskNews, '/coindesk/news')