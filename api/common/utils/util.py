import datetime
import time


# datetime时间转为字符串
def datetime_to_str(dt):
    time_str = dt.strftime('%Y-%m-%d %H:%M:%S')
    return time_str


# 字符串时间转为时间戳
def str_to_timestamp(time_str):
    timestamp = time.mktime(time.strptime(time_str, '%Y-%m-%d %H:%M:%S'))
    return timestamp


# datetime时间转为时间戳
def datetime_to_timestamp(dt):
    timestamp = time.mktime(time.strptime(dt.strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S'))
    return timestamp


# 时间戳转为datetime时间
def timestamp_to_datetime(timestamp):
    dt = datetime.datetime.fromtimestamp(timestamp)
    return dt


# uinx时间戳转换为本地时间
def localtime(timestamp):
    localtime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))
    return localtime


# 字符串时间转换函数
def datetime_to_normaltime(dt):
    normaltime = datetime.datetime.strptime(dt,'%Y-%m-%d %H:%M:%S')
    return normaltime


# 字符串转为float
def str_to_float(str1):
    if str1 is None:
        return None
    try:
        return float(str1)
    except Exception:
        return None


# 字符串转为int
def str_to_int(str1):
    if str1 is None:
        return None
    try:
        return int(str1)
    except Exception:
        return None