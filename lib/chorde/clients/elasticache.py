# -*- coding: utf-8 -*-
import memcache
import socket

from . import memcached

class ElastiCacheStoreClient(memcached.MemcachedStoreClient):
    """
    This is a low-level memcache client for use with ElastiCache, that can be passed
    to chorde.clients.memcached.MemcachedClient instead of the default memcache.Client,
    or simply use get_cluster_from_config_entrypoint to set up a regular MemcachedClient

    Tested to work with python-memcache 1.31+
    """

    FORCE_IS_DYNAMIC = True

    @classmethod
    def get_cluster_from_config_entrypoint(cls, entrypoint, fallback = None):
        c = cls([entrypoint])
        cluster_description = c.get("AmazonElastiCache:cluster")
        if not cluster_description:
            cluster_description = c.get_config("cluster")
        if cluster_description:
            cluster_description = cluster_description.split()
        if not cluster_description or len(cluster_description) <= 1:
            # Default, go directly to the configuration entry point
            return [entrypoint] if fallback is None else fallback
        else:
            cluster_nodes = cluster_description[1:]
            return [
                "%s:%d" % (host.rstrip('.'), int(port))
                for host, ip, port in [ node.split('|',2) for node in cluster_nodes ]
            ]

    def get_config(self, key):
        self.check_key(key)
        server, key = self._get_server(key)
        if not server:
            return None

        def _unsafe_get():
            self._statlog("config get")

            try:
                server.send_cmd("config get %s" % (key,))
                rkey = flags = rlen = None

                rkey, flags, rlen, = self._expectconfigvalue(server, raise_exception=True)

                if not rkey:
                    return None
                try:
                    value = self._recv_value(server, flags, rlen)
                finally:
                    server.expect("END", raise_exception=True)
            except (memcache._Error, socket.error), msg:
                if isinstance(msg, tuple): msg = msg[1]
                server.mark_dead(msg)
                return None

            return value

        try:
            return _unsafe_get()
        except memcache._ConnectionDeadError:
            # retry once
            try:
                if server.connect():
                    return _unsafe_get()
                return None
            except (memcache._ConnectionDeadError, socket.error), msg:
                server.mark_dead(msg)
            return None

    def _expectconfigvalue(self, server, line=None, raise_exception=False):
        if not line:
            line = server.readline(raise_exception)

        if line and line[:6] == "CONFIG":
            resp, rkey, flags, len = line.split()
            flags = int(flags)
            rlen = int(len)
            return (rkey, flags, rlen)
        else:
            return (None, None, None)

class ElastiCacheClientMixin:
    def expand_entry(self, entry):
        last_expansions = getattr(self, '_last_entry_expansions', None)
        if last_expansions is None:
            self._last_entry_expansions = last_expansions = {}
        get_cluster = getattr(self._client_class, 'get_cluster_from_config_entrypoint',
            ElastiCacheStoreClient.get_cluster_from_config_entrypoint)
        try:
            expanded = last_expansions[entry] = get_cluster(entry, fallback = last_expansions.get(entry))
        except:
            expanded = last_expansions.get(entry, (entry,))
        return expanded

class ElastiCacheClient(ElastiCacheClientMixin, memcached.MemcachedClient):
    pass

class FastElastiCacheClient(ElastiCacheClientMixin, memcached.FastMemcachedClient):
    pass
