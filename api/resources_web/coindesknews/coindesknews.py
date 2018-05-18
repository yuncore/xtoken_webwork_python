from flask_restful import Resource, reqparse, fields, marshal_with
from api.db import crawl_db, relation_db, app_db, AppCollections, CrawlCollections, RelationCollections, ASCENDING, DESCENDING
import json
import re


class CoindeskNews(Resource):
    get_parser = reqparse.RequestParser()
    get_parser.add_argument('time', type=float, location='args')

    def get(self):
        args = self.get_parser.parse_args()
        time = args['time']
        print(time)
        one_day_seconds = 3600 * 24
        if time is not None:
            cs = crawl_db[CrawlCollections.COINDESK_NEWS].find({'time': {'$gte': (time / 1000) - one_day_seconds * 3,
                                                                         '$lte': (time / 1000) + one_day_seconds * 3}})
            return list(cs)