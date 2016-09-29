# -*- coding: utf-8 -*-
import os
import os.path
import time
import socket
import threading

def is_ip4(x):
    try:
        socket.inet_aton(x)
        return True
    except:
        return False
try:
    # supports ipv4 and ipv6
    import ipaddr
    def is_ip(x):
        try:
            ipaddr.IPAddress(x)
            return True
        except ValueError:
            return False
    def is_ip6(x):
        try:
            ipaddr.IPv6Address(x)
            return True
        except ValueError:
            return False
except ImportError:
    import re  
    def is_ip6(x, ip6match = re.compile(r'^[0-9a-fA-F:]{6,32}$').match): # lint:ok
        return bool(ip6match(x))
    def is_ip(x):  # lint:ok
        return is_ip4(x) or is_ip6(x)

def basic_dnsquery(host, typ):
    if typ == 'A':
        expiration = time.time() + 60 # token expiration, the OS knows
        for addrinfo in socket.getaddrinfo(host, 0, socket.AF_INET, socket.SOCK_STREAM):
            ip = addrinfo[-1][0]
            yield ip, expiration
    else:
        raise NotImplementedError

def hosts_dnsquery(host, typ, hostsfile = "/etc/hosts"):
    if typ not in ('A', 'AAAA'):
        return
    if os.path.isfile(hostsfile) and os.access(hostsfile, os.R_OK):
        with open(hostsfile, "r") as f:
            for l in f:
                if host in l:
                    if '#' in l:
                        l = l.split('#',1)[0]
                    l = l.strip()
                    if l:
                        parts = filter(bool, [ x.strip() for x in l.split() ])
                        if host in parts:
                            if typ == 'A':
                                if is_ip4(parts[0]):
                                    yield (parts[0], time.time() + 60)
                            elif typ == 'AAAA':
                                if is_ip6(parts[0]):
                                    yield (parts[0], time.time() + 60)

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
            yield str(rdata).rstrip('.'), ans.expiration
except ImportError:
    import warnings
    warnings.warn("dnspython missing, will not support dynamic CNAME server lists")
    
    # basic fallback that serves to dected round-robin dns at least
    dnsquery = basic_dnsquery

def dnsquery_if_hostname(host, typ):
    if is_ip(host):
        return [(host, time.time() + 86400)]
    else:
        # Try hostfile first
        hresults = list(hosts_dnsquery(host, typ))
        if hresults:
            return hresults
        else:
            # Then real DNS
            return dnsquery(host, typ)

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
    def _client_store(self):
        return self

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
            dyn_expiration = time.time() + 60
            expiration = dyn_expiration + 86400
            allstatic = True if self._static_client_addresses is None else False
            for entry in self._client_addresses:
                if entry in static_addresses:
                    servers.append(entry)
                else:
                    if callable(entry):
                        # Custom callable, just call it
                        sentries = [ x for e in entry() for x in self.expand_entry(e) ]
                        dynamic = True
                        expiration = min(expiration, dyn_expiration)
                    else:
                        sentries = self.expand_entry(entry)
                        dynamic = False
                        if set(sentries) != set([entry]):
                            # expand_entry made it nonstatic
                            allstatic = False
                            expiration = min(expiration, dyn_expiration)

                    for entry in sentries:
                        host = self.extract_host(entry)

                        if entry and host is not None:
                            addrs = list(hosts_dnsquery(host, 'A'))
                            if addrs:
                                # Locally defined host, don't check CNAME
                                addrs = []
                            else:
                                # Check CNAME indirection
                                try:
                                    addrs = list(dnsquery_if_hostname(host, 'CNAME'))
                                except:
                                    addrs = []
                            if not addrs:
                                # Check dns round-robin
                                try:
                                    addrs = list(dnsquery_if_hostname(host, 'A'))
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
        store = self._client_store
        if store._client is None or store._client_servers != self.servers:
            store._client_servers = servers = self.servers
            store._client = self._client_class(sorted(servers), **self._client_args)
        return store._client

    @client.deleter
    def client(self):  # lint:ok
        store = self._client_store
        store._client = None
        self._dynamic_client_addresses = None
        self._dynamic_client_checktime = None

    def expand_entry(self, entry):
        return (entry,)

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

class DynamicResolvingClientLocalStore(threading.local):
    _client = None
    _client_servers = None

class ThreadLocalDynamicResolvingClient(DynamicResolvingClient):
    @property
    def _client_store(self):
        tl = getattr(self, '_tl', None)
        if tl is None:
            self._tl = tl = DynamicResolvingClientLocalStore()
        return tl
