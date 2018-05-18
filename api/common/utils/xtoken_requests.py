import requests
import time
from logconfig import run_log as LOGGER


def xtoken_get(url, **kwargs):
    rs = requests.session()
    rs.keep_alive = False
    num = 0
    page = ''
    while page == '' or num > 3:
        try:
            page = rs.get(url=url, **kwargs)
            num += 1
        except Exception as ex:
            LOGGER.error(ex)
            time.sleep(0.1)
            continue
    return page
