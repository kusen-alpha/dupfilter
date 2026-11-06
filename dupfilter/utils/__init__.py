# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2023/9/6

import base64
import hashlib
from typing import Union

SFB_CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"


def md5(s: Union[str, bytes]) -> str:
    _md5 = hashlib.md5()
    if isinstance(s, str):
        _md5.update(s.encode('utf8'))
    else:
        _md5.update(s)
    return _md5.hexdigest()


def decimal2sfb(num: int) -> str:
    if num == 0:
        return ""
    result = decimal2sfb(num // 64)
    result += SFB_CHARACTERS[num % 64]
    return result


def hex2sfb(s: str) -> str:
    """
    自定义十六进制转六十四进制，速度比hex2b64慢10倍，但能压缩少1-3个字符
    :param s:
    :return:
    """
    return decimal2sfb(int(s, 16))


def hex2b64(s: str) -> str:
    """
    十六进制转base64
    :param s:
    :return:
    """
    return base64.b64encode(bytes.fromhex(s)).decode('utf-8')


if __name__ == '__main__':
    import time

    t1 = time.time()
    for _i in range(10000):
        r = hex2sfb('698d51a19d8a121ce581499d7b701557')
    t2 = time.time()

    # 示例用法
    for _i in range(10000):
        hex2b64('698d51a19d8a121ce581499d7b701557')
    t3 = time.time()
    print(t2 - t1, t3 - t2)
