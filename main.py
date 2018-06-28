from flask import Flask
from flask_cors import CORS
# from celery import Celery
from api.config import Conf
from api.common.task.schedulertask import scheduler_task
from api.resources_web import site_blue_print
from flask_restful import request
from logconfig import run_log as LOGGER

app = Flask(__name__)
CORS(app, supports_credentials=True)
app.config.from_object(Conf)
app.register_blueprint(site_blue_print, url_prefix='/xp/site')


# scheduler_task()


@app.before_request
def before_request():
    method = request.method
    url = request.url
    LOGGER.info('method='+method + ' URL='+url)


# @app.after_request
# def after_request():
#     LOGGER.info('==================')
#     request.headers['Access-Control-Allow-Origin'] = '*'
#     request.headers['Access-Control-Allow-Methods'] = 'POST，GET,OPTIONS'
#     request.headers['Access-Control-Allow-Headers'] = 'x-requested-with,content-type'

#
# def make_celery(app):
#     celery = Celery(app.import_name, broker=app.config['CELERY_BROKER_URL'])
#     celery.conf.update(app.config)
#     TaskBase = celery.Task
#
#     class ContextTask(TaskBase):
#         abstract = True
#
#         def __call__(self, *args, **kwargs):
#             with app.app_context():
#                 return TaskBase.__call__(self, *args, **kwargs)
#     celery.Task = ContextTask
#     return celery
#
#
# celery = make_celery(app)
#
#
# @celery.task
# def add(x, y):
#     return x + y


if __name__ == '__main__':
    app.debug = Conf.DEBUG
    app.run(host='0.0.0.0', port=5050, processes=1, threaded=True)
