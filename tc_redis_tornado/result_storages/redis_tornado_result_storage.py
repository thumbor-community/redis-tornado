# -*- coding: utf-8 -*-

# Licensed under the MIT license:
# http://www.opensource.org/licenses/mit-license
# Copyright (c) 2015 Thumbor Community

import tornadis
import tornado
import time
from datetime import datetime, timedelta
from thumbor.result_storages import BaseStorage
from thumbor.utils import logger


class Storage(BaseStorage):

    pool = None

    '''start_time is used to calculate the last modified value when an item
    has no expiration date.
    '''
    start_time = None

    def __init__(self, context):
        '''Initialize the RedisStorage

        :param thumbor.context.Context shared_client: Current context
        :param boolean shared_client: When set to True a singleton client will
                                      be used.
        '''

        BaseStorage.__init__(self, context)

        if not Storage.pool:
            Storage.pool = tornadis.ClientPool(
                max_size=self.context.config.get(
                    'REDIS_TORNADO_RESULT_STORAGE_POOL_MAX_SIZE',
                    -1
                ),
                client_timeout=self.context.config.get(
                    'REDIS_TORNADO_RESULT_STORAGE_CLIENT_TIMEOUT',
                    -1
                ),
                port=self.context.config.REDIS_RESULT_STORAGE_SERVER_PORT,
                host=self.context.config.REDIS_RESULT_STORAGE_SERVER_HOST,
                # db=self.context.config.REDIS_RESULT_STORAGE_SERVER_DB,
                # password=self.context.config.REDIS_RESULT_STORAGE_SERVER_PASSWORD
            )

        if not Storage.start_time:
            Storage.start_time = time.time()

    def is_auto_webp(self):
        '''

        TODO This should be moved into the base storage class.
             It is shared with file_result_storage

        :return: If the file is a webp
        :rettype: boolean
        '''

        return self.context.config.AUTO_WEBP \
            and self.context.request.accepts_webp

    def get_key_from_request(self):
        '''Return a key for the current request url.

        :return: The storage key for the current url
        :rettype: string
        '''

        path = "result:%s" % self.context.request.url

        if self.is_auto_webp():
            path += '/webp'

        return path

    def get_max_age(self):
        '''Return the TTL of the current request.

        :returns: The TTL value for the current request.
        :rtype: int
        '''

        default_ttl = self.context.config.RESULT_STORAGE_EXPIRATION_SECONDS
        if self.context.request.max_age == 0:
            return self.context.request.max_age

        return default_ttl

    @tornado.gen.coroutine
    def put(self, bytes):
        '''Save to redis

        :param bytes: Bytes to write to the storage.
        :return: Redis key for the current url
        :rettype: string
        '''

        key = self.get_key_from_request()
        result_ttl = self.get_max_age()

        logger.debug(
            "[REDIS_RESULT_STORAGE] putting `{key}` with ttl `{ttl}`".format(
                key=key,
                ttl=result_ttl
            )
        )

        with (yield Storage.pool.connected_client()) as client:
            if result_ttl > 0:
                yield client.call('SETEX', key, result_ttl, bytes)
            else:
                yield client.call('SET', key, bytes)

        raise tornado.gen.Return(key)

    @tornado.gen.coroutine
    def get(self):
        '''Get the item from redis.'''

        key = self.get_key_from_request()
        with (yield Storage.pool.connected_client()) as client:
            result = yield client.call('GET', key)
            raise tornado.gen.Return(result)

    @tornado.gen.coroutine
    def last_updated(self):
        '''Return the last_updated time of the current request item

        :return: A DateTime object
        :rettype: datetetime.datetime
        '''

        key = self.get_key_from_request()
        max_age = self.get_max_age()

        if max_age == 0:
            raise tornado.gen.Return(
                datetime.fromtimestamp(Storage.start_time)
            )

        with (yield Storage.pool.connected_client()) as client:
            ttl = yield client.call('TTL', key)

        if ttl >= 0:
            raise tornado.gen.Return(
                datetime.now() - timedelta(seconds=(max_age - ttl))
            )

        if ttl == -1:
            # Per Redis docs: -1 is no expiry, -2 is does not exists.
            raise tornado.gen.Return(
                datetime.fromtimestamp(Storage.start_time)
            )
        # Should never reach here. It means the storage put failed or the item
        # somehow does not exists anymore.
        raise tornado.gen.Return(datetime.now())
