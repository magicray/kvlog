import os
import json
import glob
import struct
import sqlite3
import asyncio
import logging
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


STATE = dict()


async def read_http(reader):
    first = await reader.readline()

    headers = dict()
    while True:
        line = await reader.readline()
        if not line.strip():
            break
        line = line.decode().split(':', 1)
        headers[line[0].strip().lower()] = line[1].strip()

    return urllib.parse.unquote(first.decode()), headers


def write_http(writer, status, obj=None):
    writer.write('HTTP/1.0 {}\n\n'.format(status).encode())
    writer.write(
        json.dumps(obj if obj else status, indent=4, sort_keys=True).encode())


async def handler(reader, writer):
    request, headers = await read_http(reader)
    method, url, _ = request.split()

    url = url.split('/')[1:]

    if not url:
        write_http(writer, '200 OK', STATE)
        return writer.close()

    if url[0] not in STATE:
        write_http(writer, '404 Not Found')
        return writer.close()

    sql = SQLite(os.path.join('db', url[0]))
    state = STATE[url[0]]

    content_length = int(headers.get('content-length', '0'))

    # PUT
    if content_length:
        content = await reader.read(content_length)
        items = json.loads(content.decode())
        for key, value in items.items():
            key = key.encode()
            value = json.dumps(value).encode()

            sql('delete from kv where key=?', key.decode())
            sql('insert into kv values(null,?,?)', key, value)

        seq = sql('select max(seq) from kv').fetchone()[0]
        sql.commit()

        write_http(writer, '200 OK', dict(keys=len(items), seq=seq))
        log('put(%s) keys(%d) seq(%d)', url[0], len(items), seq)
        return writer.close()

    # GET
    if 2 == len(url):
        res = dict()
        for key in [k.strip() for k in url[1].split(',')]:
            k = key.encode()
            row = sql('select value from kv where key=?', k).fetchone()
            if row:
                res[key] = json.loads(row[0].decode())

        seq = sql('select max(seq) from kv').fetchone()[0]
        sql.rollback()

        write_http(writer, '200 OK', res)
        log('get(%s) keys(%d) seq(%d)', url[0], len(res), seq)
        return writer.close()

    # SYNC
    if 1 == len(url):
        while True:
            peer_term, peer_seq = struct.unpack(
                '!QQ', await reader.readexactly(16))

            log('master(%s) term(%d) seq(%d)', url[0], peer_term, peer_seq)

            myseq = sql('select max(seq) from kv').fetchone()[0]
            myterm = sql('''select seq from kv
                            where key is null order by seq desc limit 1
                         ''').fetchone()[0]
            sql.rollback()

            if (myterm, myseq) < (peer_term, peer_seq):
                return writer.close()

            next_seq = None

            if myterm == peer_term:
                next_seq = peer_seq + 1

            if myterm > peer_term:
                seq = sql('''select seq from kv where key is null and seq > ?
                             order by seq limit 1
                          ''', peer_term).fetchone()[0]

                if peer_seq > seq:
                    next_seq = seq
                else:
                    next_seq = peer_seq + 1

            if next_seq is None:
                log('this should not happend - my(%d %d) peer(%d %d)',
                    myseq, myterm, peer_seq, peer_term)
                return writer.close()

            while True:
                cur = sql('select seq, key, value from kv where seq >= ?',
                          next_seq)

                seq, count = 0, 0
                for seq, key, value in cur:
                    writer.write(struct.pack('!Q', seq))
                    writer.write(struct.pack('!Q', len(key)))
                    writer.write(key)
                    writer.write(struct.pack('!Q', len(value)))
                    writer.write(value)

                    log('master(%s) seq(%d) key(%d) value(%d)',
                        url[0], seq, len(key), len(value))

                    next_seq = seq + 1
                    count += 1

                sql.rollback()

                if 0 == count:
                    await asyncio.sleep(1)
                else:
                    writer.write(struct.pack('!Q', 0))
                    log('master(%s) next(%d) count(%d)',
                        url[0], next_seq, count)

    # Should never reach here
    assert(False)


async def sync_task(db):
    while True:
        for ip, port in STATE[db]['cluster']:
            try:
                reader, writer = await asyncio.open_connection(ip, port)
            except Exception:
                continue

            writer.write('SYNC /{} HTTP/1.1\n\n'.format(db).encode())

            sql = SQLite(os.path.join('db', db))

            seq = sql('select max(seq) from kv').fetchone()[0]
            term = sql('''select seq from kv
                          where key is null order by seq desc limit 1
                       ''').fetchone()[0]

            writer.write(struct.pack('!QQ', term, seq))

            log('slave(%s) master(%s:%d) term(%d) seq(%d)',
                db, ip, port, term, seq)

            while True:
                count = 0
                while True:
                    seq = struct.unpack('!Q', await reader.readexactly(8))[0]
                    if 0 == seq:
                        sql.commit()
                        log('slave(%s) master(%s:%d) committed(%d)',
                            db, ip, port, count)
                        break

                    key = await reader.readexactly(
                        struct.unpack('!Q', await reader.readexactly(8))[0])
                    value = await reader.readexactly(
                        struct.unpack('!Q', await reader.readexactly(8))[0])

                    sql('delete from kv where seq >= ?', seq)
                    sql('insert into kv values(?,?,?)', seq, key, value)

                    log('slave(%s) master(%s:%d) seq(%d) key(%d) value(%d)',
                        db, ip, port, seq, len(key), len(value))

                    count += 1

                await asyncio.sleep(1)

        await asyncio.sleep(1)


def server():
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.start_server(handler, args.ip, args.port))

    for db in sorted(glob.glob('db/*.sqlite3')):
        db = '.'.join(os.path.basename(db).split('.')[:-1])
        sql = SQLite(os.path.join('db', db))
        state = STATE.setdefault(db, dict())

        cluster = sql('select value from kv where seq=0').fetchone()[0]
        sql.rollback()

        state['cluster'] = sorted(json.loads(cluster))
        state['committed'] = 0
        state['followers'] = 0
        state['following'] = False

        asyncio.ensure_future(sync_task(db))
        break

    def exception_handler(loop, context):
        import pprint; pprint.pprint(context)
        os._exit(1)

    loop.set_exception_handler(exception_handler)
    loop.run_forever()


def init():
    if not os.path.isdir('db'):
        os.mkdir('db')

    sql = SQLite(os.path.join('db', args.db))
    sql('''create table if not exists kv(
        seq   integer primary key autoincrement,
        key   blob,
        value blob)''')
    sql('create unique index if not exists key on kv(key)')

    if args.cluster:
        val = json.dumps(sorted([
            (c.split(':')[0].strip(), int(c.split(':')[1].strip()))
            for c in args.cluster.split(',')]))

        sql('delete from kv where seq=0')
        sql('insert into kv values(0,null,?)', val)
        sql.commit()
    else:
        cluster = sql('select value from kv where seq=0').fetchone()[0]
        for ip, port in json.loads(cluster):
            print('{}:{}'.format(ip, port))


if __name__ == '__main__':
    # openssl req -x509 -nodes -subj / -sha256 --keyout ssl.key --out ssl.cert

    args = argparse.ArgumentParser()
    args.add_argument('--db', dest='db')
    args.add_argument('--ip', dest='ip')
    args.add_argument('--port', dest='port', type=int)
    args.add_argument('--cluster', dest='cluster')
    args = args.parse_args()

    server() if args.port else init()
