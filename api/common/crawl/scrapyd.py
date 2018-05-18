import requests
from api.config import Conf


def scrapy_add_job(payload):
    """
    向服务器添加任务
    :param payload:
    :return:
    """
    return requests.post(Conf.SCRAPYDHOST + 'schedule.json', data=payload)


def scrapy_cancel_job(payload):
    """
    取消任务
    :param payload:
    :return:
    """
    return requests.post(Conf.SCRAPYDHOST + 'cancel.json', data=payload)


def scrapy_list_job(payload):
    """
    任务列表
    :param payload:
    :return:
    """
    return requests.get(
        Conf.SCRAPYDHOST + 'listjobs.json?project={0}&spider={1}'.format(payload['project'], payload['spider']))


if __name__ == '__main__':
    res = requests.post('http://192.168.1.222:6800/schedule.json', data={'project': 'NeoScrapy', 'spider': 'coindesk'})
    print(res.text)