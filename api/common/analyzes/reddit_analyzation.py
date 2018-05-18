from api.db import stat_db, crawl_db, relation_db, CrawlCollections, StatCollections, RelationCollections, DESCENDING, AppCollections, app_db
from datetime import datetime
from bson import ObjectId
import re
import time
import pandas as pd
import numpy as np


def analyze_key_user(sr_name, source):
    """分析subreddit下的kol"""
    start = time.time()
    save_col = StatCollections.REDDIT_KEY_USER
    if source == 'link':
        col = CrawlCollections.REDDIT_LINK
        k1 = 'post_key_user'
        k2 = 'links_describe'
        k3 = 'post_key_user_describe'
    else:
        col = CrawlCollections.REDDIT_COMMENT
        k1 = 'comment_key_user'
        k2 = 'comments_describe'
        k3 = 'comment_key_user_describe'
    df = get_df(col, sr_name, ['author', 'score'])
    grouped_df = df.groupby('author', as_index=False)
    aggregated_df = grouped_df['score'].agg([np.sum, np.mean, np.size]).reset_index().sort_values(by='sum',
                                                                                                  ascending=False)
    user_count = aggregated_df.index.size
    key_user = []
    for index, row in aggregated_df[:token_count(user_count)].iterrows():
        key_user.append(row.to_dict())
    links_describe = df.describe().to_json()
    post_key_user_describe = aggregated_df.describe().to_json()
    stat_db[save_col].find_one_and_update({'_id': sr_name},
                                          {'$set': {k1: key_user,
                                                    k2: links_describe,
                                                    k3: post_key_user_describe}},
                                          upsert=True)
    end = time.time()
    print('analyze key user completed used {0}, sr_name= {1}, source={2}'.format(end - start, sr_name, source))


def kol_link_history(author):
    cs = crawl_db[CrawlCollections.REDDIT_LINK].find({'data.author': author})
    df = pd.DataFrame([i['data'] for i in cs], columns=['author', 'subreddit', 'created'])


def kol_history(author):
    start = datetime.now()
    # check update time
    u_data = stat_db[StatCollections.REDDIT_KOL_HISTORY].find_one({"_id": author})
    if u_data and (start - u_data['update_time']).seconds < 3600 * 24 * 7:
            return

    # 从reddit link中加载数据到dataframe
    link_cs = crawl_db[CrawlCollections.REDDIT_LINK].find({'data.author': author})
    link_df = pd.DataFrame([i['data'] for i in link_cs], columns=['author', 'subreddit', 'created'])
    # 分析用户在每个subreddit中的发帖数量
    link_df['num'] = 1
    link_grouped_df = link_df.groupby('subreddit', as_index=False).agg({'num': np.sum})
    link_data = link_grouped_df.sort_values(by='num', ascending=False).to_json(orient='values')

    # 从reddit comment中加载数据到dataframe
    comment_cs = crawl_db[CrawlCollections.REDDIT_COMMENT].find({'data.author': author})
    comment_df = pd.DataFrame([i['data'] for i in comment_cs], columns=['author', 'subreddit', 'created'])
    # 分析用户在每个subreddit中的回复数量
    comment_df['num'] = 1
    comment_grouped_df = comment_df.groupby('subreddit', as_index=False).agg({'num': np.sum})
    comment_data = comment_grouped_df.sort_values(by='num', ascending=False).to_json(orient='values')

    # 保存数据
    stat_db[StatCollections.REDDIT_KOL_HISTORY].find_one_and_update(
        {'_id': author},
        {'$set': {'link_data': link_data, 'comment_data': comment_data, 'update_time': datetime.now()}},
        upsert=True
    )
    end = time.time()
    print('analyze key user history completed used {0}, sr_name= {1}'.format(end - start, author))


def time_distribute(sr_name, source):
    """分析发帖和评论的日分布"""
    start = time.time()
    save_col = StatCollections.SUBREDDIT_TIME_DISTRIBUTE
    if source == 'link':
        col = CrawlCollections.REDDIT_LINK
        db_type = 'link'
    else:
        col = CrawlCollections.REDDIT_COMMENT
        db_type = 'comment'
    latest_record = crawl_db[col].find({'data.subreddit': sr_name}).sort([('data.created', DESCENDING)])
    t = latest_record[0]['data']['created'] - 180 * 24 * 3600
    df = get_df(col, sr_name, ['id', 'created'], {'data.created': {'$gt': t}}, drop_deleted=False)
    df.created = df['created'].apply(lambda x: datetime.fromtimestamp(x))
    df.set_index(df.created, inplace=True)
    df['num'] = 1
    df.drop('created', axis=1)
    df = df.resample('D').sum().fillna(0)
    data = df.reset_index().to_json(orient='values')
    stat_db[save_col].find_one_and_update({'sr_name': sr_name, 'type': db_type}, {'$set': {'data': data}}, upsert=True)
    end = time.time()
    print('analyze time distribute completed used {0}, sr_name= {1}, source={2}'.format(end - start, sr_name, source))


def word_mentioned_time_distribute(sr_name, source):
    """分析和subreddit相关的关键词在发帖和评论中的提及次数及日期分布"""
    start = time.time()
    save_col = StatCollections.REDDIT_CURRENCY_RELATED_KEY_WORD_STAT
    if source == 'link':
        col = CrawlCollections.REDDIT_LINK
        test_col = 'title'
        select_cols = ['id', 'title', 'created']
        type_flag = 'link'
    else:
        col = CrawlCollections.REDDIT_COMMENT
        test_col = 'body'
        select_cols = ['id', 'body', 'created']
        type_flag = 'comment'
    keyword_cs = app_db[AppCollections.CURRENCY_RELATED_KEY_WORD].find({'main': sr_name})
    test_str_list = list(keyword_cs)

    # 分析发帖或评论的title中包含关键词的帖子
    # 将该subreddit下所有的帖子加载到dataframe中
    df = get_df(col, 'NEO', select_cols, drop_deleted=False)
    df.created = df['created'].apply(lambda x: datetime.fromtimestamp(x))
    df.set_index(df.created, inplace=True)
    df.drop('created', axis=1)
    # 遍历和该subreddit相关的关键词，对每个关键词生成时间分布图
    for text_str in test_str_list:
        # 生成正则表达式
        text = '|'.join(text_str['test'])
        p = re.compile(r'\b({0})\b'.format(text), re.I | re.M)
        # 过滤得到将包含该关键词的记录
        df2 = df[df[test_col].str.contains(p)]
        # 根据创建时间分组聚合
        df3 = df2.resample('D').agg({'id': lambda x: len(x)})
        data = df3.reset_index().to_json(orient='values')
        stat_db[save_col].find_one_and_update(
            {'main': sr_name, 'type': type_flag, 'keyword': text_str['_id']},
            {'$set': {'data': data}},
            upsert=True
        )
    end = time.time()
    print('analyze key word completed used {0}, sr_name= {1}, source={2}'.format(end - start, sr_name, source))


def get_df(col, sr_name, columns, query=None, drop_deleted=True):
    """从数据库中加载dataframe"""
    if query is None:
        query = {}
    query['data.subreddit'] = re.compile('^{0}$'.format(sr_name), re.I)
    cs = crawl_db[col].find(query)
    df = pd.DataFrame([item['data'] for item in cs], columns=columns)
    if drop_deleted:
        df = df[df['author'] != '[deleted]']
    return df


def token_count(total):
    """选择kol的数量"""
    if total * 0.1 > 50:
        user_token = 50
    elif total * 0.1 < 10:
        user_token = 10
    else:
        user_token = int(total * 0.1)
    return user_token


def analyze_kol_batch_entry():
    """批量分析数据库中所有currency相关的subreddit中的KOL"""
    relation_cursor = relation_db[RelationCollections.RELATION_CURRENCY_REDDIT].find()
    for relation in relation_cursor:
        sr_name = relation['sr_name']
        analyze_key_user(sr_name, source='link')
        analyze_key_user(sr_name, source='comment')


def analyze_kol_history():
    """批量分析kol history的入口"""
    cs = stat_db[StatCollections.REDDIT_KEY_USER].find()
    for sr in cs:
        cku = sr['comment_key_user']
        lku = sr['post_key_user']
        if cku:
            for u in cku:
                kol_history(u['author'])
        if lku:
            for u in lku:
                kol_history(u['author'])


def analyze_time_distribute_batch_entry():
    """批量分析数据库中所有currency相关的subreddit的时间和评论日分布图"""
    relation_cursor = relation_db[RelationCollections.RELATION_CURRENCY_REDDIT].find()
    for relation in relation_cursor:
        sr_name = relation['sr_name']
        try:
            time_distribute(sr_name, source='link')
            time_distribute(sr_name, source='comment')
        except Exception as e:
            print('error!!!!' ,sr_name)


def set_neo_subproject():
    p = [
        {'full_name': 'Redpulse', 'short_name': 'RPX'},
        {'full_name': 'Zeepin', 'short_name': 'ZPT'},
        {'full_name': 'DeepBrain', 'short_name': 'DBC'},
        {'full_name': 'TheKey', 'short_name': 'TKY'},
        {'full_name': 'Qlink', 'short_name': 'QLC'},
        {'full_name': 'Elastos', 'short_name': 'ELA'},
        {'full_name': 'NEX'},
        {'full_name': 'Ontology', 'short_name': 'ONT'},
        {'full_name': 'APEX', 'short_name': 'CPX'},
        {'full_name': 'Trinity', 'short_name': 'TNC'},
        {'full_name': 'Alphacat', 'short_name': 'ACAT'},
        {'full_name': 'Orbis', 'short_name': 'OBT'},
        {'full_name': 'PeerAltas'},
        {'full_name': 'Birdge', 'short_name': 'IAM'},
        {'full_name': 'Effect.AI'},
        {'full_name': 'Thor'},
        {'full_name': 'Moonlight', 'short_name': 'LUX'},
        {'full_name': 'Wowoo', 'short_name': 'WWB'},
        {'full_name': 'Narrative', 'short_name': 'NRV'},
        {'full_name': 'Switcheo', 'short_name': 'SWH'},
    ]
    for i in p:
        update = [i['full_name']]
        if 'short_name' in i:
            update.append(i['short_name'])
            app_db[AppCollections.CURRENCY_RELATED_KEY_WORD].find_one_and_update(
                {'main': 'NEO', 'display': i['full_name'],},
                {'$set':  { 'test': update}},
                upsert=True
            )


if __name__ == '__main__':
    # analyze_kol_batch_entry()
    # word_mentioned_time_distribute('NEO', 'link')
    # word_mentioned_time_distribute('NEO', 'comment')
    analyze_time_distribute_batch_entry()