from flask_restful import Resource, reqparse, fields, marshal_with
from api.db import crawl_db, CrawlCollections, DESCENDING, relation_db, RelationCollections
import re


class RedditPost(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('sr_name', type=str, location='args')
    parser.add_argument('sort_field', type=str, location='args')
    parser.add_argument('page', type=int, location='args', default=1)
    parser.add_argument('num', type=int, location='args', default=10)
    parser.add_argument('days', type=int, location='args')

    link_fields = {
        'author': fields.String,
        'title': fields.String,
        'domain': fields.String,
        'created': fields.Float,
        'url': fields.String,
        'selftext': fields.String,
        'score': fields.Integer,
        'subreddit': fields.String,
        'num_comments': fields.Integer
    }

    def get(self, post_id=None):
        if post_id is not None:
            link = crawl_db[CrawlCollections.REDDIT_LINK].find_one({'_id': post_id})
            return self.pack(link['data'])
        else:
            args = self.parser.parse_args()
            sr_name = args['sr_name']
            days = args['days']
            query = {'data.subreddit': sr_name}
            sort_field = args['sort_field']
            num = args['num']
            page = args['page']
            if days is not None:
                latest_record = crawl_db[CrawlCollections.REDDIT_LINK].find({}).sort([('data.created', DESCENDING)])
                t = latest_record[1]['data']['created'] - days * 24 * 3600
                query['data.created'] = {'$gt': t}
            cs = crawl_db[CrawlCollections.REDDIT_LINK].find(query)
            count = cs.count()
            res = cs.sort([('data.' + sort_field, DESCENDING)]).skip((page - 1) * num).limit(num)
            return {'data': [self.pack(p['data']) for p in res], 'count': count}

    @staticmethod
    def pack(data):
        return {
            'author': data['author'],
            'title': data['title'],
            'domain': data['domain'],
            'created': data['created'],
            'url': data['url'],
            'selftext': data['selftext'],
            'score': data['score'],
            'subreddit': data['subreddit'],
            'num_comments': data['num_comments']
        }


