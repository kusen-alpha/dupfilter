# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2023/9/7
from dupfilter.filters import Filter


class RedisFilter(Filter):
    def __init__(self, server, *args, **kwargs):
        self.server = server
        super(RedisFilter, self).__init__(*args, **kwargs)
