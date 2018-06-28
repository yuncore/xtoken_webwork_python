from api.db import stat_db, crawl_db, CrawlCollections, StatCollections, relation_db, RelationCollections
import time
import re
from datetime import datetime
import pandas as pd
import numpy as np


def quote_graph(link_id):
    """
    单个btt帖子的用户关系图
    :param link_id:
    :return:
    """
    start = time.time()

    # 加载单条帖子的所有评论
    cursor = crawl_db[CrawlCollections.BTT_COMMENT].find({'data.link_id': link_id})
    comment_df = pd.DataFrame([item['data'] for item in cursor], columns=['author', 'grade', 'quote_id', 'user_id', 'message_id'])
    comment_df['num'] = 1

    # 按照用户名分组，计算每个人评论的数量
    grouped_df = comment_df.groupby(['author', 'user_id', 'grade'], as_index=False).agg(np.sum)

    # 取评论数量前100位的用户
    filtered_df = grouped_df.sort_values(by='num', ascending=False)[:100]
    describe = grouped_df.describe()

    # 将评论数量按比例转换为size，其中评论最多的人size为100
    filtered_df['num'] = filtered_df['num'] * 100 / describe['num']['max']

    # 将过滤后的data frame和原来的数据进行连接操作，目的是找出各个用户之间的引用关系
    merged_df = pd.merge(comment_df, filtered_df, on=['user_id', 'author', 'grade'], how='inner')
    temp_relation_df = merged_df.dropna().groupby(['quote_id', 'user_id'], as_index=False).agg(np.sum)

    # 将temp_relation_df中的quote_id与原数据的message_id做连接，找出user_id和quote_user_id
    relation_df = pd.merge(temp_relation_df, comment_df, left_on='quote_id', right_on='message_id', how='inner')[['user_id_x', 'user_id_y', 'num_x']]
    relation_df.columns = ['target', 'source', 'size']
    filter_user_json = filtered_df.sort_values(by='grade').to_json(orient='records')
    relation_json = relation_df.to_json(orient='records')
    category_json = filtered_df.grade.value_counts().to_json()
    stat_db[StatCollections.BTT_USER_RELATION_STAT].find_one_and_update({'_id': link_id},
                                                                        {'$set': {'user': filter_user_json,
                                                                                  'relation': relation_json,
                                                                                  'categories': category_json}},
                                                                        upsert=True)
    end = time.time()
    print('analyze bitcointalk user completed used {0}, link_id= {1}'.format(end - start, link_id))


def comment_time_distribute(link_id):
    """
    单个btt帖子时间的日回复数量
    :param link_id:
    :return:
    """
    start = time.time()

    comments = pd.DataFrame([item['data'] for item in crawl_db[CrawlCollections.BTT_COMMENT].find({'data.link_id': link_id})],
                                columns=['time'])
    comments = comments.dropna()
    comments.time = comments['time'].apply(lambda x: datetime.fromtimestamp(x))
    comments.set_index(comments.time, inplace=True)
    comments['num'] = 1
    comments.drop('time', axis=1)
    df = comments.resample('D').sum().fillna(0)
    data = df.reset_index().to_json(orient='values')
    stat_db[StatCollections.BTT_LINK_TIME_DISTRIBUTE].find_one_and_update({'_id': link_id},
                                                                          {'$set': {
                                                                              'data': data
                                                                          }},
                                                                          upsert=True)
    end = time.time()
    print('analyze comment time distribute used {0}, link_id= {1}'.format(end - start, link_id))


def search_and_stat_keyword(keywords):
    comments = crawl_db[CrawlCollections.BTT_COMMENT].find()
    keyword_collections = []
    for keyword in keywords:
        params = {}
        params['keyword'] = keyword
        params['data'] = []
        keyword_collections.append(params)
    print('获取评论...')

    # 从已有的comments中找出包含关键词的评论
    for comment in comments:
        try:
            text = comment['data']['content']
            for params in keyword_collections:
                if re.search(r'\b{0}\b'.format(params['keyword']), text, re.I) is not None:
                    params['data'].append(comment)
        except Exception:
            pass

    for index, params in enumerate(keyword_collections):
        print('分析第{0}个keyword'.format(index))
        keyword_stat(params['data'], params['keyword'])
        print('第{0}个keyword分析结束'.format(index))


def keyword_stat(select_comments, keyword):
    """
    在已经获取的所有评论中找到包含了keyword的评论，按照用户的姓名分组。找出这个人最早提到这个关键词的时间。形成时间和用户activity的分布图
    :param keyword: 查找的关键词
    :param select_comments: 搜索出来的评论
    :return:
    """

    # 将挑选出的评论加载到dataFrame中
    df = pd.DataFrame([item['data'] for item in select_comments],
                      columns=['time', 'activity', 'author', 'link_id', 'user_id', 'grade'])
    df['num'] = 1

    # 将选出的评论按照用户名和等级分组，并对time这一列取最小值，num这一列取总和，activity这一列取最大值
    group = df.groupby(['author', 'grade', 'user_id'], as_index=False)
    grouped_df = group.agg({'time': np.min, 'num': np.sum, 'activity': np.max})

    # 提及人数
    user_count = grouped_df.index.size

    # 将grade这一列作为category类型
    d = grouped_df['grade'].astype('category')

    # 将time这一列转换为时间戳
    grouped_df['time'] = grouped_df['time'] * 1000

    # 按照num的比例算每个点的大小值，最大50，最小2
    max = grouped_df['num'].max()
    min = grouped_df['num'].min()
    if max != min:
        grouped_df['num'] = grouped_df['num'].map(lambda x: np.floor((x - min) / (max - min) * 48 + 2))
    else:
        grouped_df['num'] = 50

    # 调整dataFrame中列顺序，将time放在第一列，activity放在第二列，因为在画分布图的时候，第一列的书作为横坐标，第二列的值作为纵坐标
    activity = grouped_df['activity']
    time = grouped_df['time']
    grouped_df.drop(labels=['activity'], axis=1, inplace=True)
    grouped_df.drop(labels=['time'], axis=1, inplace=True)
    grouped_df.insert(0, 'activity', activity)
    grouped_df.insert(0, 'time', time)

    # 计算top30的排名
    top30 = grouped_df.sort_values(by='time')[:30]
    top30_json = top30.to_json(orient='values')

    # 将大的dataFrame按照等级拆分为小的dataFrame，作为多个series
    json_arr = []
    for grade in d.cat.categories:
        df = grouped_df[grouped_df['grade'] == grade]
        json = df.to_json(orient='values')
        json_arr.append({'data': json, 'grade': grade})
    stat_db[StatCollections.BTT_COMMENT_KEYWORD_STAT].find_one_and_update({'keyword': keyword},
                                                    {'$set': {'data': json_arr,
                                                              'user_count': user_count,
                                                              'keyword': keyword,
                                                              'top30_data': top30_json}},
                                                    upsert=True)


def link_stat_batch(func):
    """
    批量分析用户关系图和评论时间分布图的入口
    :return:
    """
    aly_link = crawl_db[CrawlCollections.BTT_COMMENT].aggregate([{'$group': {'_id': '$data.link_id'}}])
    for item in aly_link:
        link_id = item['_id']
        if func == 'quote_graph':
            stat = stat_db[StatCollections.BTT_USER_RELATION_STAT].find_one({'_id': link_id})
            if stat is None:
                quote_graph(link_id)
        if func == 'comment_time_distribute':
            stat = stat_db[StatCollections.BTT_LINK_TIME_DISTRIBUTE].find_one({'_id': link_id})
            if stat is None:
                comment_time_distribute(link_id)


def user_history_stat():
    """
    用户历史足迹分析，找出用户在那些项目中回复的数量以及时间分布
    :return:
    """
    count = 0
    now = datetime.now()
    # 将货币和ANN link的关联关系加载到data frame
    relation_btt = relation_db[RelationCollections.RELATION_CURRENCY_BTT].find({'ann': True})
    relation_df = pd.DataFrame([item for item in relation_btt], columns=['topic_id', 'currency_name'])

    # 获取数据库中所有的用户
    user_list = crawl_db[CrawlCollections.BTT_COMMENT].aggregate([{'$group': {'_id': '$data.user_id'}}])
    for user in user_list:
        count += 1
        if count % 100 == 0:
            print('success analyse count {0}'.format(count))
        user_id = user['_id']

        # 从数据库中查找用户分析过的数据，如果不为空则跳出循环
        stat_data = stat_db[StatCollections.BTT_USER_HISTORY_STAT].find_one({'_id': user_id})
        if stat_data is not None:
            if (now - stat_data['update_time']).seconds < 3600 * 24 * 7:
                if 'data' in stat_data['arr_data']:
                    continue

        # 从数据库中加载该用户的所有评论
        df = pd.DataFrame(
            [item['data'] for item in crawl_db[CrawlCollections.BTT_COMMENT].find({'data.user_id': user_id})],
            columns=['time', 'author', 'user_id', 'link_id'])

        # 将关系data frame和
        merged_df = pd.merge(df, relation_df, left_on='link_id', right_on='topic_id', how='left')
        clean_merged_df = merged_df.dropna().drop('topic_id', axis=1)

        # 将数据按照currency name分组，获取每个currency的评论数量
        clean_merged_df['num'] = 1
        grouped_df = clean_merged_df.groupby(['link_id', 'currency_name'], as_index=False).agg({'num': np.sum})
        general_data = grouped_df.sort_values(by='num', ascending=False).to_json(orient='values')

        # 将用户的足迹按照currency name 分类, 获取评论分布图
        arr_data = []
        clean_merged_df['time'] = clean_merged_df['time'].apply(lambda x: datetime.fromtimestamp(x))
        clean_merged_df.set_index(clean_merged_df.time, inplace=True)
        clean_merged_df.drop('time', axis=1)
        d = clean_merged_df['currency_name'].astype('category')
        for currency_name in d.cat.categories:
            temp_df = clean_merged_df[clean_merged_df['currency_name'] == currency_name]
            resampled_df = temp_df.resample('D').sum().fillna(0)
            json_data = resampled_df.reset_index().to_json(orient='values')
            arr_data.append({'currency_name': currency_name, 'data': json_data})

        # 保存数据
        stat_db[StatCollections.BTT_USER_HISTORY_STAT].find_one_and_update({'_id': user_id},
                                                                           {'$set': {
                                                                               'general_data': general_data,
                                                                               'arr_data': arr_data,
                                                                               'update_time': datetime.now()
                                                                           }}, upsert=True)


def set_ann_flag():
    """
    在relation_currency_btt中的关系添加一个标志位创世帖。数据来源于relation_currency_ann表中
    :return:
    """
    ann_links = relation_db[RelationCollections.RELATION_CURRENCY_ANN].find()
    for item in ann_links:
        query = {'topic_id': item['topic_id'], 'currency_name': item['currency_name']}
        r = relation_db[RelationCollections.RELATION_CURRENCY_BTT].find_one(query)
        if r is None:
            relation_db[RelationCollections.RELATION_CURRENCY_BTT].insert_one({'topic_id': item['topic_id'], 'currency_name': item['currency_name'], 'ann': True})
        else:
            relation_db[RelationCollections.RELATION_CURRENCY_BTT].update_one(query, {'$set': {'ann': True}})


if __name__ == '__main__':
    # link_stat_batch('quote_graph')
    # link_stat_batch('comment_time_distribute')
    # user_history_stat()
    # charleshoskinson /  Jeremy Wood / founder  / IOHK
    # search_and_stat_keyword(['Jeremy Wood', 'DFINITY', 'word2vec'])
    # comment_time_distribute('1365894')
    quote_graph('1365894')
