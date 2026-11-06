# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2023/9/7

from __future__ import annotations

from typing import Any

from dupfilter.filters import Filter


class RedisFilter(Filter):
    def __init__(self, server: Any, *args: Any, **kwargs: Any) -> None:
        self.server = server
        super().__init__(*args, **kwargs)
