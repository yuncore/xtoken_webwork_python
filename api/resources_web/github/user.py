from flask_restful import Resource, reqparse, fields, marshal_with
from api.db import crawl_db, CrawlCollections, relation_db, RelationCollections


class GitHubUser(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('currency_name', type=str, location='args')
    parser.add_argument('limit', type=int, default=10, location='args')
    user_fields = {
        'id': fields.Integer,
        'login': fields.String,
        'avatar_url': fields.String,
        'followers': fields.Integer,
        'html_url': fields.String,
        'name': fields.String,
        'following': fields.Integer
    }

    def get(self):
        args = self.parser.parse_args()
        currency_name = args['currency_name']
        limit = args['limit']
        if currency_name is not None:
            return get_user_by_currency_name(currency_name, limit)


def get_user_by_currency_name(currency_name, limit):
    relation = relation_db[RelationCollections.RELAITON_CURRENCY_GITHUB].find_one({'currency_name': currency_name})
    users = []
    try:
        project = crawl_db[CrawlCollections.GIT_PROJECT].find_one({'full_name': relation['full_name']})
        commit_stat = crawl_db[CrawlCollections.GIT_STAT].find_one({'project_id': project['id'], 'type': 'sc'})
        if commit_stat is None:
            return []
        sorted_commit_stat = sorted(commit_stat['data'], key=lambda x: x['total'], reverse=True)[:limit]
        for item in sorted_commit_stat:
            user = crawl_db[CrawlCollections.GIT_CONTRIBUTOR].find_one({'id': item['author']['id']})
            if user is not None:
                item['author'] = {
                    'id': user['id'],
                    'login': user['login'],
                    'avatar_url': user['avatar_url'],
                    'followers': user['followers'],
                    'html_url': user['html_url'],
                    'name': user['name'],
                    'following': user['following']
                }
                users.append(item)
        return users
    except KeyError:
        return None
