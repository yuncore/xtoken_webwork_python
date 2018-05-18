from flask_restful import Resource, reqparse, marshal_with, fields
from api.db import crawl_db, CrawlCollections, relation_db, RelationCollections, stat_db, StatCollections, ASCENDING, \
    DESCENDING
import re


order_dict = {
    'ascending': ASCENDING,
    'descending': DESCENDING
}


class CommentDistribute(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('link_id', type=str, location='args')

    def get(self):
        args = self.parser.parse_args()
        link_id = args['link_id']
        return stat_db[StatCollections.BTT_LINK_TIME_DISTRIBUTE].find_one({'_id': link_id})


class KeyWordComment(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('keyword', type=str, location='args')

    def get(self):
        args = self.parser.parse_args()
        keyword = args['keyword']
        stat = stat_db[StatCollections.BTT_COMMENT_KEYWORD_STAT].find_one({'keyword': keyword})
        if stat is not None:
            del stat['_id']
            return stat
        return None


class Comment(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('author', type=str, required=True, location='args')
    parser.add_argument('keyword', type=str, location='args')
    parser.add_argument('page', type=int, default=1, location='args')
    parser.add_argument('num', type=int, default=20, location='args')
    parser.add_argument('col', type=str, default='data.time', location='args')
    parser.add_argument('order', type=str, default='ascending', choices=('ascending', 'descending'), location='args')

    def get(self):
        args = self.parser.parse_args()
        author = args['author']
        keyword = args['keyword']
        page = args['page']
        num = args['num']
        sort_col = args['col']
        order = args['order']
        if keyword is not None:
            return self.get_keyword_comments(author, keyword, sort_col, order)
        comments_cursor = crawl_db[CrawlCollections.BTT_COMMENT].find({'data.author': author})
        if sort_col is not None:
            comments_cursor.sort([(sort_col, comments_cursor[order])])
        count = comments_cursor.count()
        comment_list = comments_cursor.skip((page - 1) * num).limit(num)
        comment_result = []
        for comment in comment_list:
            comment_result.append(comment['data'])
        return {'count': count, 'data': comment_result}

    @staticmethod
    def get_keyword_comments(author, kw, sort_col, order):
        comments_cursor = crawl_db[CrawlCollections.BTT_COMMENT].find({'data.author': author})
        if sort_col is not None:
            comments_cursor.sort([(sort_col, order_dict[order])])
        select_comments = []
        for comment in comments_cursor:
            if comment['data']['content']:
                try:
                    text = comment['data']['content']
                    if re.search(r'\b{0}\b'.format(kw), text, re.I) is not None:
                        select_comments.append(comment['data'])
                except Exception:
                    pass
        return select_comments
