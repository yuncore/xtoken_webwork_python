from flask_restful import Resource, reqparse
from api.db import crawl_db, CrawlCollections, stat_db, StatCollections, relation_db, RelationCollections
import re


class RedditUser(Resource):
    parser = reqparse.RequestParser()

    def get(self, user_id):
        user = crawl_db[CrawlCollections.REDDIT_USER].find_one({'_id': user_id})
        return user


class RedditKeyUser(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('currency_name', type=str, required=True, location='args')

    def get(self):
        args = self.parser.parse_args()
        currency_name = args['currency_name']
        relation = relation_db[RelationCollections.RELATION_CURRENCY_REDDIT].find_one({'currency_name': currency_name})
        if relation is not None:
            key_user_stat = stat_db[StatCollections.REDDIT_KEY_USER].find_one({'_id': re.compile('^{0}$'.format(relation['sr_name']), re.I)})
            if key_user_stat is not None:
                comment_key_user = key_user_stat['comment_key_user'][:10]
                post_key_user = key_user_stat['post_key_user'][:10]
                return {'post_key_user': post_key_user, 'comment_key_user': comment_key_user}
