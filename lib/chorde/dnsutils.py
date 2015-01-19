# -*- coding: utf-8 -*-
import time
import socket
try:
    import dns.resolver
    resolver = None
    def dnsquery(host, typ):
        global resolver
        if resolver is None:
            # Lower timeout, we want to be fast
            resolver = dns.resolver.Resolver()
            resolver.lifetime = min(resolver.lifetime, 1)
        ans = resolver.query(host, typ)
        for rdata in ans:
            yield str(rdata), ans.expiration
except ImportError:
    import warnings
    warnings.warn("dnspython missing, will not support dynamic CNAME server lists")
    
    # basic fallback that serves to dected round-robin dns at least
    def dnsquery(host, typ):  # lint:ok
        if typ == 'A':
            expiration = time.time() + 60 # token expiration, the OS knows
            for addrinfo in socket.getaddrinfo(host, 0, socket.AF_INET, socket.SOCK_STREAM):
                ip = addrinfo[-1][0]
                yield ip, expiration
        else:
            raise NotImplementedError


class DynamicResolvingClient(object):
    def __init__(self, client_class, client_addresses, client_args={}):
        self._client_class = client_class
        self._client_args = client_args
        self._client_addresses = client_addresses
        self._static_client_addresses = None
        self._dynamic_client_checktime = None
        self._dynamic_client_addresses = None
        self._client = None
        self._client_servers = None
        self._removed_addresses = set()

    @property
    def servers(self):
        """
        Returns a set of server names derived from client_addresses.
        When the addresses in client_addresses point to specific hosts, this will
        just return client_addresses. But when entries in client_addresses are indirect,
        eg by specifying a dns name that resolves to a CNAME, it will dynamically update
        the returned list according to whatever the dns query returns. Entries
        can also be callables, to dynamically generate server lists using other custom procedures.
        The callable must return a list of entries that will be used as if they had been included
        in place of the callable (ie: they can also be CNAME entries).
        """
        if self._static_client_addresses is True:
            rv = self._client_addresses
        elif self._dynamic_client_addresses is not None and (self._dynamic_client_checktime or 0) > time.time():
            rv = self._dynamic_client_addresses
        else:
            # Generate dynamic list
            servers = []
            static_addresses = self._static_client_addresses or set()
            expiration = time.time() + 60
            allstatic = True if self._static_client_addresses is None else False
            for entry in self._client_addresses:
                if entry in static_addresses:
                    servers.append(entry)
                else:
                    if callable(entry):
                        # Custom callable, just call it
                        sentries = entry()
                        dynamic = True
                    else:
                        sentries = (entry,)
                        dynamic = False

                    for entry in sentries:
                        host = self.extract_host(entry)
    
                        if entry and host is not None:
                            # Check CNAME indirection
                            try:
                                addrs = list(dnsquery(host, 'CNAME'))
                            except:
                                addrs = []
                            if not addrs:
                                # Check dns round-robin
                                try:
                                    addrs = list(dnsquery(host, 'A'))
                                except:
                                    addrs = []
                                addrs = self.check_static(addrs)
                            if addrs:
                                # sort to maintain consistent hashing
                                addrs = sorted(addrs)
                        else:
                            addrs = None
                        if not addrs:
                            if not dynamic:
                                static_addresses.add(entry)
                            else:
                                allstatic = False
                            servers.append(entry)
                        else:
                            allstatic = False
                            for addr,ttl in addrs:
                                expiration = min(ttl, expiration)
                                servers.append(self.construct_host(addr, entry))
            if allstatic:
                self._static_client_addresses = True
                rv = self._client_addresses
            else:
                self._static_client_addresses = static_addresses
                
                # Schedule a recheck when TTL expires (or 5 seconds, whichever is higher)
                self._dynamic_client_checktime = max(expiration, time.time() + 5)
                self._dynamic_client_addresses = servers
                rv = servers
        return set([addr for addr in rv if addr not in self._removed_addresses])

    def remove_address(self, address):
        self._removed_addresses.add(address)

    def clear_removed_addresses(self):
        self._removed_addresses.clear()

    @property
    def client(self):
        if self._client is None or self._client_servers != self.servers:
            self._client_servers = servers = self.servers
            self._client = self._client_class(servers, **self._client_args)
        return self._client

    @client.deleter
    def client(self):  # lint:ok
        self._client = None
        self._dynamic_client_addresses = None
        self._dynamic_client_checktime = None

    def extract_host(self, entry):
        if ':' in entry:
            return entry.split(':')[0]
        else:
            return None

    def construct_host(self, host, entry):
        if ':' in entry:
            return "%s:%s" % (host, entry.split(':')[1])
        else:
            return None

    def check_static(self, addrs):
        if len(addrs) == 1:
            # normal A record, forget to mark static
            addrs = []
        return addrs
