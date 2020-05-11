import json
import urllib.parse
import urllib.request


class Client():
    def __init__(self, servers):
        self.servers = servers
        self.leader = None

    def server_list(self):
        servers = [self.leader] if self.leader else []
        servers.extend(self.servers)

        return servers

    def state(self):
        result = dict()

        for ip, port in self.servers:
            try:
                server = '{}:{}'.format(ip, port)
                with urllib.request.urlopen('http://' + server) as r:
                    result[server] = json.loads(r.read())
            except Exception:
                pass

        return result

    def put(self, key, value):
        value = value if type(value) is bytes else value.encode()

        for ip, port in self.server_list():
            try:
                url = 'http://{}:{}/{}'.format(ip, port, key)
                req = urllib.request.Request(url, data=value)
                with urllib.request.urlopen(req) as r:
                    self.leader = (ip, port)
                    return dict(status=r.headers['KVLOG_STATUS'],
                                version=r.headers['KVLOG_VERSION'],
                                committed=r.headers['KVLOG_COMMITTED'])
            except Exception:
                pass

    def get(self, key):
        for ip, port in self.server_list():
            try:
                url = 'http://{}:{}/{}'.format(ip, port, key)
                with urllib.request.urlopen(url) as r:
                    self.leader = (ip, port)
                    return dict(key=key, value=r.read(),
                                lock=r.headers['KVLOG_LOCK'],
                                version=r.headers['KVLOG_VERSION'])
            except Exception:
                pass
