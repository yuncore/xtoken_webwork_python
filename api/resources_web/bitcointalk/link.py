from flask_restful import Resource, reqparse, marshal_with, fields
from api.db import crawl_db, CrawlCollections, relation_db, RelationCollections, stat_db, StatCollections


class BTTLink(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('currency_name', type=str, location='args')
    resource_fields = {
        'title': fields.String,
        'replies': fields.Integer,
        'views': fields.Integer,
        'link_url': fields.String,
        'profile_url': fields.String,
        'started_by': fields.String,
        'user_id': fields.String,
        'id': fields.String
    }

    @marshal_with(resource_fields, envelope='data')
    def get(self, link_id=None):
        """
        根据给定link_id的值获取帖子的基本信息或者根据currency_name获取和该currency相关的贴子
        :param link_id:
        :return:
        """
        if link_id is not None:
            link = crawl_db[CrawlCollections.BTT_LINK].find_one({'_id': link_id})
            return link
        else:
            args = self.parser.parse_args()
            currency_name = args['currency_name']
            if currency_name is not None:
                links = related_btt(currency_name)
                # sort_links = sorted(links, key=lambda link: link['replies'], reverse=True)[:10]
                return links


class BTTRelationUserLinks(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('currency_name', type=str, location='args')

    def get(self):
        """
        根据currency_name获取已分析过用户评论关联关系的帖子
        :return:
        """
        args = self.parser.parse_args()
        currency_name = args['currency_name']
        if currency_name is not None:
            links = related_btt(currency_name)
            link_ids = [item['id'] for item in links]
            query = {'_id': {'$in': link_ids}}
            cursor = stat_db[StatCollections.BTT_USER_RELATION_STAT].find(query)
            stated_link = []
            for item in cursor:
                link = crawl_db[CrawlCollections.BTT_LINK].find_one({'_id': item['_id']})
                if link is not None:
                    stated_link.append(link['data'])
            return stated_link


class BTTTimeDistributeLinks(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('currency_name', type=str, location='args')

    def get(self):
        """
        根据currency_name获取已分析过的评论随时间分布的帖子
        :return:
        """
        args = self.parser.parse_args()
        currency_name = args['currency_name']
        if currency_name is not None:
            links = related_btt(currency_name)
            link_ids = [item['id'] for item in links]
            query = {'_id': {'$in': link_ids}}
            stated_link = []
            cursor = stat_db[StatCollections.BTT_LINK_TIME_DISTRIBUTE].find(query)
            for item in cursor:
                link = crawl_db[CrawlCollections.BTT_LINK].find_one({'_id': item['_id']})
                if link is not None:
                    stated_link.append(link['data'])
            return stated_link


def related_btt(currency_name):
    relations = relation_db[RelationCollections.RELATION_CURRENCY_BTT].find({'currency_name': currency_name, 'ann': True})
    btt_links = []
    for relation in relations:
        btt_link = crawl_db[CrawlCollections.BTT_LINK].find_one({'_id': relation['topic_id']})
        if btt_link is not None:
            btt_link['data']['id'] = btt_link['_id']
            btt_links.append(btt_link['data'])
    return btt_links


if __name__ == '__main__':
    cursor = relation_db[RelationCollections.RELATION_CURRENCY_ANN].find()
    for item in cursor:
        r = relation_db[RelationCollections.RELATION_CURRENCY_BTT].find_one({'topic_id': item['topic_id'], 'currency_name': item['currency_name']})

