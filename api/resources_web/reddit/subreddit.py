from flask_restful import Resource, reqparse, fields, marshal_with
from api.db import crawl_db, CrawlCollections, relation_db, RelationCollections
import re


parser = reqparse.RequestParser()
parser.add_argument('currency_name', type=str, location='args')
resource_fields = {
    'description': fields.String,
    'subscribers': fields.Integer,
    'created': fields.String,
    'title': fields.String,
    'accounts_active': fields.Integer,
    'url': fields.String,
    'public_description': fields.String,
    'display_name': fields.String
}


class RedditSubreddit(Resource):

    @marshal_with(resource_fields, envelope='data')
    def get(self, sr_id=None):
        if sr_id is not None:
            sr = crawl_db[CrawlCollections.REDDIT_SUBREDDIT].find_one({'_id': sr_id})
            return sr['data']
        else:
            args = parser.parse_args()
            currency_name = args['currency_name']
            return related_sr(currency_name)


def related_sr(currency_name):
    relations = relation_db[RelationCollections.RELATION_CURRENCY_REDDIT].find({'currency_name': currency_name})
    sr_list = []
    for item in relations:
        sr = crawl_db[CrawlCollections.REDDIT_SUBREDDIT].find_one({'data.display_name': re.compile('^{0}$'.format(item['sr_name']), re.I)})
        if sr is not None:
            sr_list.append(sr['data'])
    return sr_list
