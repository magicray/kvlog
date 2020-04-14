import os
import json
import glob
import time
import signal
import struct
import sqlite3
import asyncio
import hashlib
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


def quorum(state):
    return int((len(args.peers) + 1) / 2)


def has_quorum(state):
    return len(state['followers']) >= quorum(state)


def committed_seq(state):
    if not has_quorum(state):
        return 0

    commits = sorted(list(state['followers'].values()), reverse=True)

    return commits[quorum(state) - 1]


def term_and_seq(sql):
    seq = sql('select max(seq) from kv').fetchone()[0]
    term = sql('''select seq from kv
                  where key is null order by seq desc limit 1
               ''').fetchone()[0]

    return term, seq


async def handler(reader, writer):
    request, headers = await read_http(reader)
    method, url, _ = request.split()

    url = url.strip('/')

    if not url:
        write_http(writer, '200 OK', {k: (v['role'], committed_seq(STATE[k]))
                                      for k, v in STATE.items()})
        return writer.close()

    url = url.split('/')

    if url[0] not in STATE:
        write_http(writer, '404 Not Found')
        return writer.close()

    sql = SQLite(os.path.join('db', url[0]))
    state = STATE[url[0]]

    peername = writer.get_extra_info('peername')
    sockname = writer.get_extra_info('sockname')

    if method.lower() in ('get', 'put', 'post'):
        no_quorum = not has_quorum(state)
        not_a_leader = 'leader' != state['role']
        is_a_follower = 'following' in state

        if not_a_leader or is_a_follower or no_quorum:
            write_http(writer, '503 Service Unavailable', state)
            return writer.close()

    content_length = int(headers.get('content-length', '0'))

    # PUT
    if method.lower() in ('put', 'post') and content_length:
        content = await reader.read(content_length)
        items = json.loads(content.decode())

        for key, value in items.items():
            sql('insert into kv values(null,?,?)',
                key.encode(),
                json.dumps(value).encode())

        seq = sql('select max(seq) from kv').fetchone()[0]
        sql.commit()

        write_http(writer, '200 OK', dict(keys=len(items), seq=seq))
        log('put(%s) keys(%d) seq(%d)', url[0], len(items), seq)
        return writer.close()

    # GET
    if method.lower() in ('get') and 2 == len(url):
        res = dict()
        for key in [k.strip() for k in url[1].split(',')]:
            row = sql('''select value from kv
                         where key=?
                         order by seq desc limit 1
                      ''', key.encode()).fetchone()
            if row:
                res[key] = json.loads(row[0].decode())

        seq = sql('select max(seq) from kv').fetchone()[0]
        sql.rollback()

        write_http(writer, '200 OK', res)
        log('get(%s) keys(%d) seq(%d)', url[0], len(res), seq)
        return writer.close()

    assert(method.lower() in ('sync'))

    def log_prefix():
        i = 1 if '127.0.0.1' == sockname[0] else 0
        return 'master({}) slave({}) role({}) db({})'.format(
            sockname[i], peername[i], state['role'], url[0])

    # SYNC
    try:
        peer_term, peer_seq = struct.unpack(
            '!QQ', await reader.readexactly(16))
        peer_chksum = await reader.readexactly(16)

        log('%s term(%d) seq(%d)', log_prefix(), peer_term, peer_seq)

        myterm, myseq = term_and_seq(sql)

        assert((myterm, myseq, state['chksum']) >
               (peer_term, peer_seq, peer_chksum))

        if myterm == peer_term:
            next_seq = peer_seq

        if myterm > peer_term:
            seq = sql('''select seq from kv where key is null and seq > ?
                         order by seq limit 1
                      ''', peer_term).fetchone()[0]

            next_seq = min(seq, peer_seq)

        sql.rollback()

        # All good. sync can start now
        state['followers'][peername] = 0
        writer.write(struct.pack('!Q', 0))

        while True:
            cur = sql('select seq,key,value from kv where seq >= ?', next_seq)

            count = 0
            for seq, key, value in cur:
                writer.write(struct.pack('!Q', seq))

                key = key if key else b''
                writer.write(struct.pack('!Q', len(key)))
                writer.write(key)

                value = value if value else b''
                writer.write(struct.pack('!Q', len(value)))
                writer.write(value)

                log('%s seq(%d) key(%d) value(%d)',
                    log_prefix(), seq, len(key), len(value))

                count += 1

            sql.rollback()

            writer.write(struct.pack('!Q', 0))

            peer_term, peer_seq = struct.unpack(
                '!QQ', await reader.readexactly(16))

            assert('following' not in state)
            state['followers'][peername] = peer_seq

            next_seq = peer_seq + 1

            if 'voter' == state['role'] and has_quorum(state):
                max_seq = sql('select max(seq) from kv').fetchone()[0]

                if max_seq == committed_seq(state):
                    sql('insert into kv values(null, null, ?)', '{}:{}'.format(
                        sockname, int(time.time()*10**9)).encode())
                    state['role'] = 'candidate'

                sql.commit()
            elif 'candidate' == state['role'] and has_quorum(state):
                max_seq = sql('select max(seq) from kv').fetchone()[0]

                if max_seq == committed_seq(state):
                    state['role'] = 'leader'

                sql.rollback()

            if count or 'candidate' == state['role']:
                log('%s term(%d) seq(%d) sent(%d)',
                    log_prefix(), peer_term, peer_seq, count)
            else:
                await asyncio.sleep(1)
    except Exception:
        sql.rollback()
        state['followers'].pop(peername, None)
        log('%s quorum(%s)', log_prefix(), has_quorum(state))

        if 'voter' != state['role'] and has_quorum(state) is False:
            log('%s quorum lost. exiting..', log_prefix())
            os._exit(1)

    sql.rollback()
    writer.close()


async def sync_task(db):
    sql = SQLite(os.path.join('db', db))
    state = STATE[db]

    while True:
        if has_quorum(state):
            return

        for ip, port in args.peers:
            try:
                reader, writer = await asyncio.open_connection(ip, port)

                peername = writer.get_extra_info('peername')
                sockname = writer.get_extra_info('sockname')
            except Exception:
                reader = None
                continue

            log_prefix = 'slave({}) master({}) db({})'.format(
                sockname[1 if '127.0.0.1' == sockname[0] else 0],
                peername[1 if '127.0.0.1' == peername[0] else 0],
                db)

            try:
                writer.write('SYNC /{} HTTP/1.1\n\n'.format(db).encode())

                term, seq = term_and_seq(sql)

                writer.write(struct.pack('!QQ', term, seq))
                writer.write(state['chksum'])

                log('%s term(%d) seq(%d)', log_prefix, term, seq)

                await reader.readexactly(8)
                break
            except Exception:
                reader = None
                writer.close()

        if reader:
            break

        await asyncio.sleep(1)

    assert(0 == len(state['followers']))
    state['following'] = (ip, port)

    while True:
        count = 0
        while True:
            seq = struct.unpack('!Q', await reader.readexactly(8))[0]

            if 0 == seq:
                break

            key = await reader.readexactly(
                struct.unpack('!Q', await reader.readexactly(8))[0])
            value = await reader.readexactly(
                struct.unpack('!Q', await reader.readexactly(8))[0])

            sql('delete from kv where seq >= ?', seq)
            sql('insert into kv values(?,?,?)', seq,
                key if key else None, value if value else None)

            log('%s seq(%d) key(%d) value(%d)',
                log_prefix, seq, len(key), len(value))

            count += 1

        term, seq = term_and_seq(sql)
        sql.commit()

        if count:
            log('%s term(%d) seq(%d) received(%d)',
                log_prefix, term, seq, count)

        writer.write(struct.pack('!QQ', term, seq))


def server():
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')
    signal.alarm(args.timeout)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.start_server(handler, '', args.port))

    args.peers = sorted([(ip.strip(), int(port)) for ip, port in [
        p.split(':') for p in args.peers.split(',')]])

    for db in sorted(glob.glob('db/*.sqlite3')):
        db = '.'.join(os.path.basename(db).split('.')[:-1])
        # sql = SQLite(os.path.join('db', db))
        state = STATE.setdefault(db, dict())

        # cluster = sql('select value from kv where seq=0').fetchone()[0]
        # sql.rollback()

        state['role'] = 'voter'
        state['followers'] = dict()
        state['chksum'] = hashlib.md5((db + str(args.peers)).encode()).digest()

        asyncio.ensure_future(sync_task(db))

    def exception_handler(loop, context):
        log(context['future'])
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
    sql('create index if not exists key on kv(key)')

    sql('delete from kv where seq=1')
    sql('insert into kv values(1, null, ?)', args.password)
    sql.commit()


if __name__ == '__main__':
    # openssl req -x509 -nodes -subj / -sha256 --keyout ssl.key --out ssl.cert

    args = argparse.ArgumentParser()
    args.add_argument('--port', dest='port', type=int)
    args.add_argument('--peers', dest='peers')
    args.add_argument('--token', dest='token',
                      default=os.getenv('KEYVALUESTORE', 'keyvaluestore'))
    args.add_argument('--timeout', dest='timeout', type=int,
                      default=int(time.time()*10**9 % 60))

    args.add_argument('--db', dest='db')
    args.add_argument('--password', dest='password')
    args = args.parse_args()

    server() if args.port else init()
