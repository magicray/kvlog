import os
import json
import glob
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

    peername = writer.get_extra_info('peername')
    sockname = writer.get_extra_info('sockname')

    content_length = int(headers.get('content-length', '0'))

    # PUT
    if content_length:
        content = await reader.read(content_length)
        items = json.loads(content.decode())
        for key, value in items.items():
            key = key.encode()
            value = json.dumps(value).encode()

            sql('delete from kv where key=?', key)
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

    def committed_seq():
        idx = quorum(state)

        if idx < len(state['followers']):
            return 0

        return sorted(list(state['followers'].values()), reverse=True)[idx-1]

    # SYNC
    try:
        assert('following' not in state)

        peer_term, peer_seq = struct.unpack(
            '!QQ', await reader.readexactly(16))
        peer_chksum = (await reader.readexactly(32)).decode()

        log('master%s slave%s quorum(%s) db(%s) term(%d) seq(%d)',
            sockname, peername, has_quorum(state), url[0], peer_term, peer_seq)

        myseq = sql('select max(seq) from kv').fetchone()[0]
        myterm = sql('''select seq from kv
                        where key is null order by seq desc limit 1
                     ''').fetchone()[0]
        sql.rollback()

        mychksum = state['chksum']
        if (myterm, myseq, mychksum) <= (peer_term, peer_seq, peer_chksum):
            if len(state['followers']) < len(args.peers)/2:
                return writer.close()

        next_seq = None

        if myterm == peer_term:
            next_seq = peer_seq + 1

        if myterm > peer_term:
            seq = sql('''select seq from kv where key is null and seq > ?
                         order by seq limit 1
                      ''', peer_term).fetchone()[0]

            if peer_seq >= seq:
                next_seq = seq
            else:
                next_seq = peer_seq + 1

        if next_seq is None:
            log('this should not happend - my(%d %d) peer(%d %d)',
                myseq, myterm, peer_seq, peer_term)
            return writer.close()

        # All good. sync can start now
        writer.write(struct.pack('!Q', 0))

        while True:
            cur = sql('select seq, key, value from kv where seq >= ?',
                      next_seq)

            seq, count = 0, 0
            for seq, key, value in cur:
                writer.write(struct.pack('!Q', seq))

                key = key if key else b''
                writer.write(struct.pack('!Q', len(key)))
                writer.write(key)

                value = value if value else b''
                writer.write(struct.pack('!Q', len(value)))
                writer.write(value)

                log('master%s slave%s db(%s) seq(%d) key(%d) value(%d)',
                    sockname, peername, url[0], seq, len(key), len(value))

                count += 1

            sql.rollback()

            assert('following' not in state)

            writer.write(struct.pack('!Q', 0))
            next_seq = struct.unpack('!Q', await reader.readexactly(8))[0]
            state['followers'][peername] = next_seq - 1

            if 'voter' == state['role'] and has_quorum(state):
                max_seq = sql('select max(seq) from kv').fetchone()[0]

                if max_seq == committed_seq():
                    sql('insert into kv values(null, null, null)')
                    state['role'] = 'candidate'

                sql.commit()
            elif 'candidate' == state['role'] and has_quorum(state):
                max_seq = sql('select max(seq) from kv').fetchone()[0]

                if max_seq == committed_seq():
                    state['role'] = 'leader'

                sql.rollback()

            log('master%s slave%s role(%s) db(%s) next(%d) count(%d)',
                sockname, peername, state['role'], url[0], next_seq, count)

            if 0 == count:
                await asyncio.sleep(1)
    except Exception:
        state['followers'].pop(peername, None)
        log('master%s slave%s quorum(%s) db(%s)',
            sockname, peername, has_quorum(state), url[0])
        if 'voter' != state['role'] and has_quorum(state) is False:
            os._exit(1)

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

            try:
                writer.write('SYNC /{} HTTP/1.1\n\n'.format(db).encode())

                seq = sql('select max(seq) from kv').fetchone()[0]
                term = sql('''select seq from kv
                              where key is null order by seq desc limit 1
                           ''').fetchone()[0]

                writer.write(struct.pack('!QQ', term, seq))
                writer.write(state['chksum'].encode())

                log('slave%s master%s db(%s) term(%d) seq(%d)',
                    sockname, peername, db, term, seq)

                await reader.readexactly(8)
                break
            except Exception:
                reader = None
                writer.close()

        if reader:
            break

        await asyncio.sleep(1)

    state['following'] = (ip, port)
    while True:
        while True:
            seq = struct.unpack('!Q', await reader.readexactly(8))[0]

            if 0 == seq:
                break

            key = await reader.readexactly(
                struct.unpack('!Q', await reader.readexactly(8))[0])
            value = await reader.readexactly(
                struct.unpack('!Q', await reader.readexactly(8))[0])

            if key:
                sql('delete from kv where key=?', key)

            sql('delete from kv where seq >= ?', seq)
            sql('insert into kv values(?,?,?)', seq,
                key if key else None, value if value else None)

            log('slave%s master%s db(%s) seq(%d) key(%d) value(%d)',
                sockname, peername, db, seq, len(key), len(value))

        sql.commit()
        seq = sql('select max(seq) from kv').fetchone()[0]
        log('slave%s master%s db(%s) committed(%d)',
            sockname, peername, db, seq)
        writer.write(struct.pack('!Q', seq+1))


def server():
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')
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
        state['chksum'] = hashlib.md5((
            db + str(args.peers)).encode()).hexdigest()

        asyncio.ensure_future(sync_task(db))
        break

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
    sql('create unique index if not exists key on kv(key)')

    sql('delete from kv where seq=0')
    sql('insert into kv values(0, null, ?)', args.password)
    sql.commit()


if __name__ == '__main__':
    # openssl req -x509 -nodes -subj / -sha256 --keyout ssl.key --out ssl.cert

    args = argparse.ArgumentParser()
    args.add_argument('--port', dest='port', type=int)
    args.add_argument('--peers', dest='peers')
    args.add_argument('--token', dest='token')

    args.add_argument('--db', dest='db')
    args.add_argument('--password', dest='password')
    args = args.parse_args()

    server() if args.port else init()
