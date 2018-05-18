import json
from logconfig import run_log as LOGGER


class ResultMsg(object):
    errorCode = 0
    errorMsg = ''
    result = ''

    @staticmethod
    def create_success_msg(result: object) -> object:
        msg = ResultMsg()
        msg.errorCode = 0
        msg.result = result
        rs = json.dumps(msg, default=lambda obj: obj.__dict__, sort_keys=False,
                        ensure_ascii=False)
        LOGGER.info(rs)
        return rs

    @staticmethod
    def create_error_msg(error_code: object, error_msg: object) -> object:
        msg = ResultMsg()
        msg.errorCode = error_code
        msg.errorMsg = error_msg
        rs = json.dumps(msg, default=lambda obj: obj.__dict__, ensure_ascii=False,
                        sort_keys=False)
        LOGGER.info(rs)
        return rs
