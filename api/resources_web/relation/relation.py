from flask_restful import Resource, reqparse, fields, marshal_with
from api.db import crawl_db, relation_db, CrawlCollections, RelationCollections, ASCENDING, DESCENDING
import json
import re


class GitHubCurrencyRelation(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('currency_name', type=str, required=True,location='args')
    parser.add_argument('full_name', type=str, required=True,location='args')

    def get(self):
        args = self.parser.parse_args()
        currency_name = args['currency_name']
        full_name = args['full_name']
        item = {
            'currency_name': currency_name,
            'full_name': full_name
        }
        relation_db[RelationCollections.RELAITON_CURRENCY_GITHUB].find_one_and_update({'full_name': full_name,
                                                                                       'currency_name': currency_name},
                                                                                      {'$set': item},
                                                                                      upsert=True)