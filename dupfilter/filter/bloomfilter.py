# -*- coding:utf-8 -*-
# author: kusen
# email: 1194542196@qq.com
# date: 2023/9/6

import random

import cachetools

from dupfilter import utils
from dupfilter.filter import Filter

EXISTS_SCRIPT = """
local key = KEYS[1]
local offsets = ARGV
local exist = true
for _, offset in pairs(offsets) do
    exist = redis.call('GETBIT', key, offset)
    if not exist then
        break
    end
end
return exist

"""
INSERT_SCRIPT = """
local key = KEYS[1]
local offsets = ARGV
for _, offset in pairs(offsets) do
    redis.call('SETBIT', key, offset, 1)
end
return true
"""

RESET_SCRIPT = """
local key = KEYS[1]
local offsets = ARGV
for _, offset in pairs(offsets) do
    redis.call('SETBIT', key, offset, 0)
end
return true
"""


class SimpleHash(object):
    def __init__(self, bit, seed):
        self.bit = bit
        self.seed = seed

    def hash(self, value):
        ret = 0
        for i in range(len(value)):
            ret += self.seed * ret + ord(value[i])
        return self.bit & ret


class BloomFilter(Filter):
    def __init__(self, server, key, bit=32, hash_num=6, block_num=1,
                 reset=False, reset_proportion=0.8, reset_check_period=7200,
                 *args, **kwargs):
        """

        :param server: The redis obj, redis.Redis()
        :param key:
        :param bit:
        :param hash_num:
        :param block_num:
        :param reset: 是否进行重置/清除，
        :param reset_proportion: 达到比例后进行重置/清除，
        :param reset_check_period: 重置/清除比例获取计算周期，
        """
        self.key = key
        if bit > 32:
            raise ValueError('The bit value not > 32!')
        self.bit = (1 << bit) - 1  # Redis的String类型最大容量为512M，即bit=32
        self.hash_num = hash_num
        self.seeds = range(1, hash_num + 1)
        self.shs = [SimpleHash(self.bit, seed) for seed in self.seeds]
        self.block_num = block_num
        self.reset = reset
        self.reset_proportion = reset_proportion
        super(BloomFilter, self).__init__(server, *args, **kwargs)
        self.exists_script = self.server.register_script(EXISTS_SCRIPT)
        self.insert_script = self.server.register_script(INSERT_SCRIPT)
        self.reset_script = self.server.register_script(RESET_SCRIPT)
        self.reset_info = cachetools.TTLCache(maxsize=2, ttl=reset_check_period)
        self.reset_info['proportion'] = self._get_used_proportion(self.key + '0')

    def exists(self, value):
        if not value:
            return False
        block, offsets = self._get_block_and_offsets(value)
        try:
            return bool(self.exists_script(keys=[self.key + block], args=offsets))
        except Exception as e:
            print(e)
            return False

    def insert(self, value):
        if not value:
            return False
        block, offsets = self._get_block_and_offsets(value)
        self._reset(block, offsets)
        try:
            return bool(self.insert_script(keys=[self.key + block], args=offsets))
        except Exception:
            return False

    def _get_used_proportion(self, key):
        return self._bit_count(key) / self.bit

    def _reset(self, current_block, current_offsets):
        proportion = self.reset_info['proportion']
        block, offsets = self._get_reset_block_and_offsets(
            current_block, current_offsets)
        key = self.key + block
        if not proportion:
            self.reset_info['proportion'] = self._get_used_proportion(key)
            return
        if proportion < self.reset_proportion:
            return
        self.reset_script(keys=[key], args=offsets)

    def _bit_count(self, key):
        return self.server.bitcount(key)

    def _get_block_and_offsets(self, value):
        value = self._value_hash(value)
        block = str(int(value[0:2], 16) % self.block_num)
        return block, [sh.hash(self._value_compress(value)) for sh in self.shs]

    def _get_reset_block_and_offsets(self, current_block, current_offsets):
        while True:
            block = str(random.choice(range(self.block_num)))
            if self.block_num == 1:
                break
            if block != current_block:
                break
        offsets = []
        random_end = self.block_num + 1
        for offset in current_offsets:
            offsets.append(
                (offset + random.choice(range(1, random_end))) % self.bit
            )
        return block, offsets


class AsyncBloomFilter(BloomFilter):
    async def insert(self, value):
        pass

    async def exists(self, value):
        pass


if __name__ == '__main__':
    bf = BloomFilter({}, '1', value_compress_func=utils.hex2b64)
    bf.insert("123")
