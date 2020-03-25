import os
import time
import json
import glob
import sqlite3
import asyncio
import logging
import hashlib
import argparse
import urllib.parse

from logging import critical as log


class SQLite():
    def __init__(self, name):
        self.conn = None
        self.path = name
        self.path += '.sqlite3' if not name.endswith('.sqlite3') else ''

    def __call__(self, query, *params):
        if self.conn is None:
            self.conn = sqlite3.connect(self.path)

        return self.conn.execute(query, params)

    def commit(self):
        if self.conn:
            self.conn.commit()
            self.rollback()

    def rollback(self):
        if self.conn:
            self.conn.rollback()
            self.conn.close()
            self.conn = None

    def __del__(self):
        self.rollback()


class g:
    state = dict()


async def read_http(reader):
    first = await reader.readline()

    headers = dict()
    while True:
        line = await reader.readline()
        if not line.strip():
            break
        line = line.decode().split(':', 1)
        headers[line[0].strip().lower()] = line[1].strip()

    return first.decode(), headers


async def handler(reader, writer):
    request, headers = await read_http(reader)
    method, url, _ = request.split()

    url = url.split('/')[1:]
    sql = SQLite(os.path.join('db', url[0]))

    content_length = int(headers.get('content-length', '0'))

    if content_length:
        content = await reader.read(content_length)
        ts = int(time.strftime('%Y%m%d%H%M%S'))
        term = 0
        for key, value in json.loads(content.decode()).items():
            sql('delete from kv where key=?', key)
            sql('insert into kv values(null,?,?,?,?)', term, ts, key, value)
        sql.commit()

        writer.write(b'HTTP/1.0 200 OK\n\n')
    elif 2 == len(url):
        res = dict()
        for k in [k.strip() for k in url[1].split(',')]:
            res[k] = sql('select value from kv where key=?', k).fetchone()[0]
        del(sql)

        writer.write(b'HTTP/1.0 200 OK\n\n')
        writer.write(json.dumps(res).encode())
    else:
        g.state[sql.path]['followers'] += 1

        writer.write(b'HTTP/1.0 200 OK\n\n')
        log('got request')
        while True:
            writer.write(json.dumps(g.state[sql.path]).encode())
            await writer.drain()
            await asyncio.sleep(1)

    writer.close()


async def sync_task(db):
    sql = SQLite(db)
    cluster = sql('select value from kv where seq=0').fetchone()[0]
    cluster = json.loads(cluster)
    del(sql)

    order = dict((hashlib.md5((ip + str(port) + db).encode()).hexdigest(),
                  (ip, port)) for ip, port in cluster)

    while True:
        reader, writer = None, None

        for chksum in sorted(order):
            ip, port = order[chksum]
            try:
                reader, writer = await asyncio.open_connection(ip, int(port))
                break
            except Exception:
                await asyncio.sleep(0.1)
                continue

        if reader and writer:
            break

    db = g.state[db]
    writer.write('GET /{} http/1.1\n\n'.format(db['name']).encode())
    first, headers = await read_http(reader)

    while True:
        log('here')
        line = await reader.readline()
        log('here 1')
        log((ip, port, json.loads(line)))


def server():
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.start_server(handler, '', args.port))

    for db in sorted(glob.glob('db/*.sqlite3')):
        g.state[db] = dict(
            name='.'.join(os.path.basename(db).split('.')[:-1]),
            followers=0)

        asyncio.ensure_future(sync_task(db))
        break

    loop.run_forever()


def init():
    if not os.path.isdir('db'):
        os.mkdir('db')

    sql = SQLite(os.path.join('db', args.db))
    sql('''create table if not exists kv(
        seq   integer primary key autoincrement,
        term  integer,
        ts    integer,
        key   text,
        value blob)''')
    sql('create unique index if not exists key on kv(key)')

    if args.cluster:
        ts = int(time.strftime('%Y%m%d%H%M%S'))
        val = json.dumps(sorted([
            (c.split(':')[0].strip(), int(c.split(':')[1].strip()))
            for c in args.cluster.split(',')]))

        sql('delete from kv where seq=0')
        sql('insert into kv values(?,?,?,?,?)', 0, 0, ts, 'config.json', val)
        sql.commit()
    else:
        cluster = sql('select value from kv where seq=0').fetchone()[0]
        for ip, port in json.loads(cluster):
            print('{}:{}'.format(ip, port))


if __name__ == '__main__':
    # openssl req -x509 -nodes -subj / -sha256 --keyout ssl.key --out ssl.cert

    args = argparse.ArgumentParser()
    args.add_argument('--db', dest='db')
    args.add_argument('--port', dest='port', type=int)
    args.add_argument('--cluster', dest='cluster')
    args = args.parse_args()

    server() if args.port else init()
