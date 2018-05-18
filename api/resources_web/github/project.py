from flask_restful import Resource, reqparse, fields, marshal_with
from api.db import crawl_db, CrawlCollections, relation_db, RelationCollections


parser = reqparse.RequestParser()
parser.add_argument('currency_name', type=str, location='args')
resource_fields = {
    'id': fields.Integer,
    'name': fields.String,
    'subscribers_count': fields.Integer,
    'open_issues_count': fields.Integer,
    'watchers': fields.Integer,
    'forks': fields.Integer,
    'language': fields.String,
    'full_name': fields.String,
    'description': fields.String
}


class GithubProject(Resource):

    @marshal_with(resource_fields, envelope='data')
    def get(self, project_id=None):
        if project_id is not None:
            return crawl_db[CrawlCollections.GIT_PROJECT].find_one({'id': project_id})
        else:
            args = parser.parse_args()
            currency_name = args['currency_name']
            return related_project(currency_name)


def related_project(currency_name):
    relations = relation_db[RelationCollections.RELAITON_CURRENCY_GITHUB].find({'currency_name': currency_name})
    projects = []
    for relation in relations:
        project = crawl_db[CrawlCollections.GIT_PROJECT].find_one({'full_name': relation['full_name']})
        if project is not None:
            projects.append(project)
    return projects