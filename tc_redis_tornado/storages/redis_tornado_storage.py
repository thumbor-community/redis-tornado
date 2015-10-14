# -*- coding: utf-8 -*-

import tornadis
import tornado
from thumbor.storages import BaseStorage


class Storage(BaseStorage):

    pool = None

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
                    'REDIS_TORNADO_STORAGE_POOL_MAX_SIZE',
                    -1
                ),
                client_timeout=self.context.config.get(
                    'REDIS_TORNADO_STORAGE_CLIENT_TIMEOUT',
                    -1
                ),
                port=self.context.config.REDIS_STORAGE_SERVER_PORT,
                host=self.context.config.REDIS_STORAGE_SERVER_HOST,
                # db=self.context.config.REDIS_STORAGE_SERVER_DB,
                # password=self.context.config.REDIS_STORAGE_SERVER_PASSWORD
            )

    @tornado.gen.coroutine
    def put(self, path, bytes):
        ttl = self.context.config.STORAGE_EXPIRATION_SECONDS

        with (yield Storage.pool.connected_client()) as client:
            if ttl:
                yield client.call('SETEX', path, ttl, bytes)
            else:
                yield client.call('SET', path, bytes)

    @tornado.gen.coroutine
    def exists(self, path):
        with (yield Storage.pool.connected_client()) as client:
            raise tornado.gen.Return(client.call('EXISTS', path))

    @tornado.gen.coroutine
    def remove(self, path):
        with (yield Storage.pool.connected_client()) as client:
            raise tornado.gen.Return(client.call('DEL', path))

    @tornado.gen.coroutine
    def get(self, path):
        with (yield Storage.pool.connected_client()) as client:
            buffer = yield client.call('GET', path)
            raise tornado.gen.Return(buffer)
