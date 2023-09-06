# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2023/9/6

import hashlib
import base64

SFB_CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"


def md5(s):
    _md5 = hashlib.md5()
    _md5.update(s.encode('utf8'))
    return _md5.hexdigest()


def decimal2sfb(num):
    if num == 0:
        return ""
    result = decimal2sfb(num // 64)
    result += SFB_CHARACTERS[num % 64]
    return result


def hex2sfb(s):
    """
    自定义十六进制转六十四进制，速度比hex2b64慢10倍，但能压缩少1-3个字符
    :param s:
    :return:
    """
    return decimal2sfb(int(s, 16))


def hex2b64(s):
    """
    十六进制转base64
    :param s:
    :return:
    """
    return base64.b64encode(bytes.fromhex(s)).decode('utf-8')


if __name__ == '__main__':
    import time

    t1 = time.time()
    for i in range(10000):
        r = hex2sfb('698d51a19d8a121ce581499d7b701557')
    t2 = time.time()

    # 示例用法
    for i in range(10000):
        hex2b64('698d51a19d8a121ce581499d7b701557')
    t3 = time.time()
    print(t2 - t1, t3 - t2)
