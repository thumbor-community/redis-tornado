# -*- coding: utf-8 -*-

# Licensed under the MIT license:
# http://www.opensource.org/licenses/mit-license
# Copyright (c) 2015 Thumbor Community

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
                password=self.context.config.REDIS_STORAGE_SERVER_PASSWORD
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
            exists = yield client.call('EXISTS', path)
            raise tornado.gen.Return(exists)

    @tornado.gen.coroutine
    def remove(self, path):
        with (yield Storage.pool.connected_client()) as client:
            yield client.call('DEL', path)

    @tornado.gen.coroutine
    def get(self, path):
        with (yield Storage.pool.connected_client()) as client:
            buffer = yield client.call('GET', path)
            raise tornado.gen.Return(buffer)

    @tornado.gen.coroutine
    def put_crypto(self, path):
        if not self.context.config.STORES_CRYPTO_KEY_FOR_EACH_IMAGE:
            return

        if not self.context.server.security_key:
            raise RuntimeError(
                "STORES_CRYPTO_KEY_FOR_EACH_IMAGE can't be True if no "
                "SECURITY_KEY specified"
            )

        key = self.__key_for(path)
        with (yield Storage.pool.connected_client()) as client:
            yield client.call('SET', key, self.context.server.security_key)

    def __key_for(self, url):
        return 'thumbor-crypto-%s' % url

    def __detector_key_for(self, url):
        return 'thumbor-detector-%s' % url
