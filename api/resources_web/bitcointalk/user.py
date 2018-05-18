from flask_restful import Resource, reqparse, marshal_with, fields
from api.db import crawl_db, CrawlCollections, stat_db, StatCollections


class BTTUserHistory(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('user_id', type=str, required=True,location='args')

    def get(self):
        args = self.parser.parse_args()
        user_id = args['user_id']
        history = stat_db[StatCollections.BTT_USER_HISTORY_STAT].find_one({'_id': user_id})
        if history is not None:
            del history['update_time']
        return history
