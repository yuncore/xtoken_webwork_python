from pymongo import MongoClient


class Config(object):
    REDDIT_CLIENT_ID = 'xxx'
    REDDIT_SECRET_KEY = 'xxx'


class DevelopmentConfig(Config):
    DEBUG = True


class ProductionConfig(Config):
    DEBUG = False


Conf = DevelopmentConfig
