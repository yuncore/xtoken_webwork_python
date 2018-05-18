from flask_restful import Resource, reqparse, fields, marshal_with, request
from api.db import crawl_db, relation_db, app_db, AppCollections, CrawlCollections, RelationCollections, \
    stat_db, StatCollections, ASCENDING, DESCENDING
from api.config import Conf
from bson.objectid import ObjectId
import json
import re
import requests
# from requests import Request,Session
from api.common.utils.resultmsg import ResultMsg
from logconfig import run_log as LOGGER
from api.common.utils.xtoken_requests import xtoken_get

currency_fields = {
    'rank': fields.Integer,
    'id': fields.String,
    'name': fields.String,
    'symbol': fields.String,
    'market_cap_usd': fields.Float,
    'percent_change_1h': fields.Float,
    'percent_change_7d': fields.Float,
    'percent_change_24h': fields.Float,
    '24h_volume_usd': fields.Float,
    'price_btc': fields.Float,
    'price_usd': fields.Float,
    'total_supply': fields.Float,
    'available_supply': fields.Float,
    'max_supply': fields.Float,
    'last_updated': fields.Float,
}
list_fields = {
    'rank': fields.Integer,
    'id': fields.String,
    'name': fields.String,
    'symbol': fields.String,
    'market_cap_usd': fields.Float,
    'percent_change_1h': fields.Float,
    'percent_change_7d': fields.Float,
    'percent_change_24h': fields.Float,
    '24h_volume_usd': fields.Float,
    'price_btc': fields.Float,
    'price_usd': fields.Float,
    'total_supply': fields.Float,
    'available_supply': fields.Float,
    'max_supply': fields.Float,
    'last_updated': fields.Float,
    'btt': fields.Integer,
    'github': fields.String,
    'reddit': fields.String
}

order_dict = {
    'ascending': ASCENDING,
    'descending': DESCENDING
}


class CurrencyList(Resource):
    BILLION = 1000000000
    MILLION = 1000000
    queries = {
        'market_cap_usd': [
            {'$lte': MILLION},
            {'$lte': 10 * MILLION, '$gt': MILLION},
            {'$lte': 100 * MILLION, '$gt': 10 * MILLION},
            {'$lte': BILLION, '$gt': 100 * MILLION},
            {'$gt': BILLION},
        ],
        'price_usd': [
            {'$lte': 0.01},
            {'$lte': 1, '$gt': 0.01},
            {'$lte': 100, '$gt': 1},
            {'$gt': 100},
        ]
    }
    list_parser = reqparse.RequestParser()
    list_parser.add_argument('page', type=int, default=1, location='args')
    list_parser.add_argument('num', type=int, default=10, location='args')
    list_parser.add_argument('col', type=str, default='rank', location='args')
    list_parser.add_argument('order', type=str, default='ascending', choices=('ascending', 'descending'),
                             location='args')
    list_parser.add_argument('filters', type=str, location='args')
    list_parser.add_argument('tag_ids', type=str, location='args')

    def get(self):
        """api for get sorted,paginated currency list.
        :return: list
        """
        args = self.list_parser.parse_args()
        page = args['page']
        num = args['num']
        sort_col = args['col']
        order = args['order']
        filters_str = args['filters']
        tag_ids = json.loads(args['tag_ids'])
        # 查询对象
        # query = {'_id': {}}
        query = {}
        black_list = []
        # 向java端查询黑名单
        try:
            # res = requests.get(Conf.JAVA_DOMAIN + '/pros/black/list/all', headers={
            #     'Connection': 'close',
            #     'source': request.headers.get('source'),
            #     'token': request.headers.get('token')
            # })
            res = xtoken_get(Conf.JAVA_DOMAIN + '/pros/black/list/all', headers={
                'source': request.headers.get('source'),
                'token': request.headers.get('token')
            })

            black_list = [i['currency'] for i in res.json()['result']]
            # query['_id']['$not'] = {'$in': black_list}
        except Exception as e:
            LOGGER.error(e)

        if len(black_list) > 0:
            query['_id'] = {}
            query['_id']['$not'] = {'$in': black_list}

        # filter_str表示过滤条件，过滤条件分为两种，一种是定义好的范围{col: '', index: ''}，
        # 一种是自定义范围{'col': '', 'custom_choice': {'start': '', 'end': ''}}。
        if filters_str is not None:
            filters = json.loads(filters_str)
            for item in filters:
                filter_col = item['col']
                if 'custom_choice' in item:
                    # 自定义范围
                    start = item['custom_choice']['start']
                    end = item['custom_choice']['end']
                    query[filter_col] = {'$lte': end, '$gt': start}
                else:
                    # 将定义好的范围改为对应的条件
                    index = item['index']
                    query[filter_col] = self.queries[filter_col][index]

        # sort_col 表示排序字段，当排序字段存在的时候需要将该字段职位None的记录过滤掉
        if sort_col is not None:
            query[sort_col] = {'$ne': None}

        # 如果tag_ids不为空说明用户选择了相应的tags作为过滤字段，需要从relation集合中将符合条件的id先查出来，使用$in操作符作为条件之一
        if len(tag_ids) > 0:
            if '_id' not in query:
                query['_id'] = {}
            query['_id']['$in'] = self.get_tag_related_currency_ids(tag_ids)

        # 查询操作
        cursor = crawl_db[CrawlCollections.COINMARKET_CURRENCY_PRICE].find(query)
        count = cursor.count()

        # 对查询的结果进行排序
        if sort_col is not None:
            cursor.sort([(sort_col, order_dict[order])])
        currency_list = cursor.skip((page - 1) * num).limit(num)

        currencies = []
        for currency in currency_list:
            # 从get_relation_fields方法中获取该currency的相关字段
            currencies.append(get_relation_fields(currency))
        # return {'data': currencies, 'count': count}
        result = {
            'data': currencies,
            'count': count
        }
        return ResultMsg.create_success_msg(result)

    @staticmethod
    def get_tag_related_currency_ids(tag_ids):
        object_tag_ids = [ObjectId(i) for i in tag_ids]
        relations = relation_db[RelationCollections.RELATION_CURRENCY_TAG].find({'tag_id': {'$in': object_tag_ids}})
        return [r['currency_id'] for r in relations]


class Currency(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('ids', type=str, location='args')
    parser.add_argument('name', type=str, location='args')

    def get(self):
        """api for get currencies by their id. value of args['id'] is converted from js array, which should not be None.
        :return: list
        """
        args = self.parser.parse_args()
        ids = args['ids']
        name = args['name']
        if ids is not None:
            return self.get_currencies_by_ids(ids)
        elif name is not None:
            return self.get_currencies_by_name(name)

    @staticmethod
    def get_currencies_by_name(name):
        return crawl_db[CrawlCollections.COINMARKET_CURRENCY_PRICE].find_one({'name': name})

    @staticmethod
    def get_currencies_by_ids(ids):
        ids = json.loads(ids)
        currencies = []
        for currency_id in ids:
            currency = crawl_db[CrawlCollections.COINMARKET_CURRENCY_PRICE].find_one({'id': currency_id})
            if currency is not None:
                currencies.append(get_relation_fields(currency))
        return currencies


class CurrencyPrice(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('id', type=str, required=True, location='args')

    def get(self):
        """
        get currency current price and currency history price
        :return:
        """
        args = self.parser.parse_args()
        currency_id = args['id']
        current_price = crawl_db[CrawlCollections.COINMARKET_CURRENCY_PRICE].find_one({'_id': currency_id})
        history_price = crawl_db[CrawlCollections.COINMARKET_HISTORY_PRICE].find_one({'_id': currency_id})
        return {'current_price': [current_price], 'history_price': history_price}


class CurrencyAdditionInfo(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('name', type=str, required=True, location='args')

    def get(self):
        args = self.parser.parse_args()
        currency_name = args['name']
        res = {}
        # mytoken 上的众筹信息
        # 根据name获取symbol
        currency = crawl_db[CrawlCollections.COINMARKET_CURRENCY_PRICE].find_one({'name': currency_name})
        if currency:
            symbol = currency['symbol']
            mytoken_data = crawl_db[CrawlCollections.ICOHOLDER_MYTOKEN].find_one({'symbol': symbol})
            if mytoken_data:
                del mytoken_data['_id']
                res['mytoken'] = mytoken_data
        return res


class CurrencySearch(Resource):
    resource_fields = {
        'name': fields.String,
        'symbol': fields.String,
        'id': fields.String
    }
    parser = reqparse.RequestParser()
    parser.add_argument('keyword', type=str, default=None, location='args')

    @marshal_with(resource_fields, envelope='data')
    def get(self):
        """ get currencies which id filed contains some words.
        :return: list
        """
        args = self.parser.parse_args()
        kw = args['keyword']
        if kw is not None:
            cursor = crawl_db[CrawlCollections.COINMARKET_CURRENCY_PRICE].find({'$or': [{"id": re.compile(kw, re.I)},
                                                                                    {'symbol': re.compile(kw, re.I)}]}).limit(100)
            return list(cursor)


class CurrencyHistoryRank(Resource):
    """货币历史排名信息"""
    parser = reqparse.RequestParser()
    parser.add_argument('id', type=str, default=None, location='args')

    def get(self):
        args = self.parser.parse_args()
        c_id = args['id']
        data = stat_db[StatCollections.HISTORY_CURRENCY_RANK].find_one({'currency_id': c_id})
        if data:
            return data['rank_market_cap']


def get_relation_fields(currency):
    """ append relation github, reddit, bitcointalk, xtoken information to the params.
    :param currency: single record from NeoData[COINMARKET_CURRENCY_PRICE].
    :return: currency contains additional information.
    """
    del currency['_id']
    currency['github'] = related_github(currency['name'])
    currency['reddit'] = related_reddit(currency['name'])
    currency['btt'] = related_btt(currency['name'])
    currency['xtoken'] = None
    currency['tags'] = related_tags(currency['id'])
    related_ico = related_ico_raised(currency['name'])
    currency['ico_raised'] = related_ico['ico_raised']
    currency['ico_date_from'] = related_ico['ico_date_from']
    currency['ico_date_to'] = related_ico['ico_date_to']
    return currency


def related_github(currency_name):
    relation = relation_db[RelationCollections.RELAITON_CURRENCY_GITHUB].find_one({'currency_name': currency_name})
    if relation is not None:
        git_project = crawl_db[CrawlCollections.GIT_PROJECT].find_one({'full_name': relation['full_name']})
        if git_project is not None:
            return {
                'id': git_project['id'],
                'name': git_project['name'],
                'open_issues_count': git_project['open_issues_count'],
                'watchers_count': git_project['watchers_count'],
                'subscribers_count': git_project['subscribers_count'],
                'network_count': git_project['network_count'],
                'full_name': git_project['full_name'],
            }


def related_reddit(currency_name):
    relation = relation_db[RelationCollections.RELATION_CURRENCY_REDDIT].find_one({'currency_name': currency_name})
    if relation is not None:
        subreddit = crawl_db[CrawlCollections.REDDIT_SUBREDDIT].find_one({'data.display_name': re.compile('^{0}$'.format(relation['sr_name']), re.I)})
        if subreddit is not None:
            return {
                'accounts_active': subreddit['data']['accounts_active'],
                'subscribers': subreddit['data']['subscribers'],
                'sr_name': subreddit['data']['display_name']
            }


def related_btt(currency_name):
    relation = relation_db[RelationCollections.RELATION_CURRENCY_BTT].find_one({'currency_name': currency_name, 'ann': True})
    if relation is not None:
        ann_link = crawl_db[CrawlCollections.BTT_LINK].find_one({'_id': relation['topic_id']})
        if ann_link is not None:
            return {
                'title': ann_link['data']['title'],
                'started_by': ann_link['data']['started_by'],
                'replies': ann_link['data']['replies'],
                'views': ann_link['data']['views']
            }


def related_tags(currency_id):
    relations = relation_db[RelationCollections.RELATION_CURRENCY_TAG].find({'currency_id': currency_id})
    return [str(r['tag_id']) for r in relations]


def related_ico_raised(currency_name):
    data = crawl_db[CrawlCollections.ICOHOLDER_BASE].find_one({'name': currency_name})
    ico_date_from = None
    ico_date_to = None
    ico_raised = None
    if data:
        ico_date_from = data['ico_date_from']
        ico_date_to = data['ico_date_to']
        ico_raised = data['ico_raised']
    return {'ico_date_from': ico_date_from,
            'ico_date_to': ico_date_to,
            'ico_raised': ico_raised}


if __name__ == '__main__':
    cursor = crawl_db[CrawlCollections.COINMARKET_CURRENCY_PRICE].find(
        {'market_cap_usd': {'$gt': 1000000, '$lte': 1000000000}})
    print(cursor.count())
