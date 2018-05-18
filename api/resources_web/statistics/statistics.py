from flask_restful import Resource, reqparse, fields, marshal_with
from api.db import stat_db, crawl_db, relation_db, StatCollections, CrawlCollections, RelationCollections, ASCENDING, DESCENDING, app_db, AppCollections
from bson import ObjectId
from api.common.utils.util import timestamp_to_datetime
import json
import re


class KeywordList(Resource):
    """获取关键词列表"""

    parser = reqparse.RequestParser()

    def get(self):
        cursor = stat_db[StatCollections.BTT_COMMENT_KEYWORD_STAT].find()
        cursor.sort([('user_count', DESCENDING)])
        keywords = []
        for item in cursor:
            data = {}
            data['keyword'] = item['keyword']
            data['user_count'] = item['user_count']
            keywords.append(data)
        return keywords


class KolUserList(Resource):
    """bitcointalk中的KOL用户列表"""

    parser = reqparse.RequestParser()
    parser.add_argument('page', type=int, default=1, location='args')
    parser.add_argument('num', type=int, default=10, location='args')

    def get(self):
        args = self.parser.parse_args()
        page = args['page']
        num = args['num']
        cursor = stat_db[StatCollections.BTT_KOL].find()
        cursor.sort([('num', DESCENDING)])
        count = cursor.count()
        user_list = cursor.skip((page - 1) * num).limit(num)
        users = []
        for user in user_list:
            comment = crawl_db[CrawlCollections.BTT_COMMENT].find_one({'data.user_id': user['user_id']})
            users.append({'user_id': user['user_id'],
                          'user_name': comment['data']['author'],
                          'grade': comment['data']['grade'],
                          'activity': comment['data']['activity'],
                          'num': user['num']})
        return {'count': count, 'data': users}


class BTTUserRelations(Resource):
    """根据用户id获取用户关系图数据"""

    parser = reqparse.RequestParser()
    parser.add_argument('link_id', type=str, location='args')

    def get(self):
        args = self.parser.parse_args()
        link_id = args['link_id']
        return stat_db[StatCollections.BTT_USER_RELATION_STAT].find_one({'_id': link_id})


class HistoryCurrencyRank(Resource):
    """根据time时间戳获取当天的货币价格排行列表"""

    parser = reqparse.RequestParser()
    parser.add_argument('time', type=float, location='args')

    def get(self):
        args = self.parser.parse_args()
        time_long = args['time']
        record = stat_db[StatCollections.HISTORY_CURRENCY_PRICE_STAT].find_one({"_id": time_long})
        return {'data': record}


class HistoryTotalMarket(Resource):
    """获取总市值变化表"""

    def get(self):
        res = crawl_db[CrawlCollections.COINMARKET_TOTAL_MARKET_CAP].find_one({'type': 'MARKETCAP_TOTAL'})
        return res['data']


class SubredditTimeDistribute(Resource):
    """单个subreddit中发帖和评论日分布数据"""

    parser = reqparse.RequestParser()
    parser.add_argument('type', type=str, location='args', required=True)
    parser.add_argument('sr_name', type=str, location='args', required=True)

    def get(self):
        args = self.parser.parse_args()
        t = args['type']
        sr_name = args['sr_name']
        data = stat_db[StatCollections.SUBREDDIT_TIME_DISTRIBUTE].find_one({'sr_name': sr_name, 'type': t})
        if data:
            return data['data']


class RedditRelatedKeyWordList(Resource):
    """和subreddit相关的关键词列表"""

    parser = reqparse.RequestParser()
    parser.add_argument('sr_name', type=str, location='args', required=True)

    def get(self):
        args = self.parser.parse_args()
        sr_name = args['sr_name']
        cs = app_db[AppCollections.CURRENCY_RELATED_KEY_WORD].find({'main': sr_name})
        res = []
        for i in cs:
            data_link = stat_db[StatCollections.REDDIT_CURRENCY_RELATED_KEY_WORD_STAT].find_one(
                {'keyword': i['_id'], 'type': 'link'}
            )
            data_comment = stat_db[StatCollections.REDDIT_CURRENCY_RELATED_KEY_WORD_STAT].find_one(
                {'keyword': i['_id'], 'type': 'comment'}
            )
            res.append([i['display'], i['main'], data_link['data'], data_comment['data']])
        return res


class RedditRelatedKeyWordAnalyzation(Resource):
    """某个关键词在reddit中发帖或评论中的分析结果"""

    parser = reqparse.RequestParser()
    parser.add_argument('keyword', type=str, location='args', required=True)

    def get(self):
        args = self.parser.parse_args()
        keyword = args['keyword']
        data_link = stat_db[StatCollections.REDDIT_CURRENCY_RELATED_KEY_WORD_STAT].find_one(
            {'keyword': ObjectId(keyword), 'type': 'link'}
        )
        data_comment = stat_db[StatCollections.REDDIT_CURRENCY_RELATED_KEY_WORD_STAT].find_one(
            {'keyword': ObjectId(keyword), 'type': 'comment'}
        )
        d = {}
        if data_link:
            d['link'] = data_link['data']
        if data_comment:
            d['comment'] = data_comment['data']
        return d


if __name__ == '__main__':
    cs = crawl_db[CrawlCollections.REDDIT_LINK].find({'data.subreddit': 'NEO'}).sort([('data.created', DESCENDING)]).limit(10)
    print([(timestamp_to_datetime(i['data']['created']), i['data']['created']) for i in cs])