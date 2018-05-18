from flask_restful import Resource, reqparse, fields, marshal_with
from api.db import crawl_db, CrawlCollections

parser = reqparse.RequestParser()
parser.add_argument('type', type=str, required=True, location='args')
parser.add_argument('project_id', type=int, required=True, location='args')


class GithubStat(Resource):
    def get(self):
        args = parser.parse_args()
        stat_type = args['type']
        project_id = args['project_id']
        stat = crawl_db[CrawlCollections.GIT_STAT].find_one({'project_id': project_id, 'type': stat_type})
        if stat is not None:
            return stat['data']
