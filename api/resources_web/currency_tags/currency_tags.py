from flask_restful import Resource, reqparse, fields, marshal_with, abort
from api.db import crawl_db, relation_db, CrawlCollections, RelationCollections, ASCENDING, DESCENDING, app_db, \
    AppCollections
from bson.objectid import ObjectId
import json
from api.common.utils.resultmsg import ResultMsg


class CurrencyTags(Resource):
    delete_parse = reqparse.RequestParser()
    delete_parse.add_argument('id', type=str, required=True)

    post_parse = reqparse.RequestParser()
    post_parse.add_argument('tag_name', type=str, required=True)

    def get(self):
        """
        获取tag列表
        :return:
        """
        tags_cursor = app_db[AppCollections.CURRENCY_TAGS].find()
        result = [{"id": str(tag['_id']), "tag_name": tag['name']} for tag in tags_cursor]
        return ResultMsg.create_success_msg(result)
        # return [{"id": str(tag['_id']), "tag_name": tag['name']} for tag in tags_cursor]

    def delete(self):
        args = self.delete_parse.parse_args()
        app_db[AppCollections.CURRENCY_TAGS].remove({'_id': ObjectId(args['id'])})

    def post(self):
        args = self.post_parse.parse_args()
        tag_records = app_db[AppCollections.CURRENCY_TAGS].find_one({'name': args['tag_name']})
        if tag_records is not None:
            abort(400, message='标签内容已存在')
        app_db[AppCollections.CURRENCY_TAGS].insert_one({'name': args['tag_name']})


class CurrencyTagRelation(Resource):
    get_parse = reqparse.RequestParser()
    get_parse.add_argument('currency_id', type=str, required=True, location='args')

    post_parse = reqparse.RequestParser()
    post_parse.add_argument('tag_ids', type=str, required=True)
    post_parse.add_argument('currency_ids', type=str, required=True)

    def get(self):
        args = self.get_parse.parse_args()
        currency_id = args['currency_id']
        relation_cursor = relation_db[RelationCollections.RELATION_CURRENCY_TAG].find({'currency_id': currency_id})
        tags_ids = [ObjectId(r['tag_id']) for r in relation_cursor]
        tags_cursor = app_db[AppCollections.CURRENCY_TAGS].find({'_id': {'$in': tags_ids}})
        return [{"id": str(tag['_id']), "tag_name": tag['name']} for tag in tags_cursor]

    def post(self):
        """
        添加货币和标签的关联关系， 参数tag_ids, 和currency_ids都是数组,理论上可以实现多对多的关联，但是实际上每次请求应该保证其中一个参数的长度为1
        """
        args = self.post_parse.parse_args()
        tag_ids = json.loads(args['tag_ids'])
        currency_id = args['currency_ids']
        relation_db[RelationCollections.RELATION_CURRENCY_TAG].remove({'currency_id': currency_id})
        for tag_id in tag_ids:
            object_tag_id = ObjectId(tag_id)
            relation_db[RelationCollections.RELATION_CURRENCY_TAG].find_one_and_update({'tag_id': object_tag_id,
                                                                                        'currency_id': currency_id},
                                                                                       {'$set':
                                                                                            {'tag_id': object_tag_id,
                                                                                             'currency_id': currency_id}},
                                                                                       upsert=True)
