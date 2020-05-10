import os
import json
import time
import pprint
import struct
import sqlite3
import asyncio
import hashlib
import logging
import argparse
import mimetypes
import urllib.parse
import urllib.request

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


def write_http(writer, status, obj=b'', headers=dict()):
    content = obj

    if type(obj) is not bytes:
        content = json.dumps(obj, indent=4, sort_keys=True).encode()
        headers['Content-Type'] = 'application/json'

    headers['Content-Length'] = len(content)

    writer.write('HTTP/1.0 {}\n'.format(status).encode())

    for k, v in headers.items():
        writer.write('{}: {}\n'.format(k, str(v)).encode())

    writer.write(b'\n')
    writer.write(content)


def quorum():
    # Peers are the servers, other than this, in the cluster.
    # To be a leader, we want at least half of the peers to follow.
    # 1 peers -> 1 follower,  2 peers -> 1 follower
    # 3 peers -> 2 followers, 4 peers -> 2 followers

    # This number + leader is a majority
    return int((len(args.peers) + 1) / 2)


def has_quorum():
    # We are good to go if we already got sync requests from a quorum or more
    return len(args.state['followers']) >= quorum()


def committed_seq():
    # It is pointless till we have a quorum
    if not has_quorum():
        return 0

    # If this node is a leader, it's max(seq) is guranteed to be biggest
    commits = sorted(list(args.state['followers'].values()), reverse=True)

    return commits[quorum() - 1]


def term_and_seq(sql):
    # Obviously, the biggest seq number
    seq = sql('select max(seq) from kv').fetchone()[0]

    # A new term starts when a candidate, after getting a quroum, inserts
    # a row with null key in the log. The seq for this row is the new term.
    #
    # This is the most critical step in the leader election process. A quorum
    # of followers must replicate this row in their logs to enable this
    # candidate to declare itself the leader for this new term.
    term = sql('''select seq from kv
                  where key is null
                  order by seq desc
                  limit 1
               ''').fetchone()[0]

    return term, seq


def panic(cond, msg):
    if cond:
        log('panic(%s)', msg)
        os._exit(1)


async def handler(reader, writer):
    request, headers = await read_http(reader)
    method, url, _ = request.split()

    url = [u for u in url.split('/') if u]
    method = method.lower()

    url.extend([None, None, None])
    req_key, req_version, req_lock = url[0:3]
    req_content = await reader.read(int(headers.get('content-length', '0')))

    req_key = req_key if req_key else None
    req_version = int(req_version) if req_version else 0
    if req_key and req_key.isdigit():
        req_key = int(req_key)

    sql = SQLite(args.db)
    state = args.state
    term, seq = term_and_seq(sql)
    sql.rollback()

    # Current leader for each db managed by this cluster
    if 'get' == method and req_key is None:
        write_http(writer, '200 OK', dict(
            role=state['role'], term=term, seq=seq,
            committed=committed_seq()))
        return writer.close()

    peername = writer.get_extra_info('peername')
    sockname = writer.get_extra_info('sockname')

    # Don't read/write until elected leader or lost quorum
    if method in ('get', 'put', 'post'):
        if 'leader' != state['role'] or has_quorum() is False:
            write_http(writer, '503 Service Unavailable')
            return writer.close()

    log_prefix = 'client({}:{})'.format(peername[0], peername[1])

    # APPEND
    if method in ('put', 'post') and type(req_key) is str:
        state['txns'][peername] = (req_key, req_version, req_content)

        # Let's batch requests to limit the number of transactions
        await asyncio.sleep(0.05)

        # No one else has processed this batch so far
        if type(state['txns'][peername]) is tuple:
            term, seq = term_and_seq(sql)

            for sock in state['txns']:
                if type(state['txns'][sock]) is not tuple:
                    continue

                key, version, value = state['txns'][sock]

                if version:
                    seq = sql('''select max(seq) from kv where key=?
                              ''', key).fetchone()[0]

                    # Conflict as version already updated
                    if version != seq:
                        state['txns'][sock] = dict(
                            status='conflict',
                            key=key, version=version, bytes=len(value),
                            existing_version=seq)
                        continue

                seq += 1
                sql('insert into kv values(?, null, ?, ?, ?)', seq, key, value,
                    str((sockname, seq, int(time.time()*10**9), os.getpid())))

                state['txns'][sock] = dict(status='ok', version=seq)

            term, seq = term_and_seq(sql)

            panic('leader' != state['role'],
                  "{} can't commit".format(state['role']))

            sql.commit()

        # Someone has committed this batch by now. Lets examine the result.
        result = state['txns'].pop(peername)

        # Great - our request was not rejected due to a conflict.
        # Let's wait till a quorum replicates the data in their logs
        if 'ok' == result['status']:
            while result['version'] > committed_seq():
                await asyncio.sleep(0.05)

        result['committed'] = committed_seq()

        write_http(writer, '200 OK', value, {
            'KVLOG_STATUS': result['status'],
            'KVLOG_VERSION': result.get('existing_version', result['version']),
            'KVLOG_COMMITTED': result['committed']})

        log('%s put(%s)', log_prefix, str(result))

        return writer.close()

    # READ
    if 'get' == method and type(req_key) is str:
        # Must not return data that is not replicated by a quorum
        commit_seq = committed_seq()

        row = sql('''select seq, lock, value from kv
                     where key=? and seq <= ?
                     order by seq desc limit 1
                  ''', req_key, commit_seq).fetchone()

        seq, lock, value = row if row else (0, '', b'')
        sql.rollback()

        mime = mimetypes.guess_type(req_key)[0]

        write_http(writer, '200 OK', value, {
            'KVLOG_KEY': req_key,
            'KVLOG_LOCK': lock if lock else '',
            'KVLOG_VERSION': seq,
            'KVLOG_COMMITTED': commit_seq,
            'Content-Type': mime if mime else 'application/octet-stream'})

        log('%s key(%d) version(%d) bytes(%d)', log_prefix, req_key, version,
            len(value))

        return writer.close()

    if 'get' == method and type(req_key) is int:
        # Must not return data that is not replicated by a quorum
        commit_seq = committed_seq()

        # key is actually a key
        if key == req_key:
            row = sql('''select seq, value from kv
                         where key=? and seq <= ? and lock is null
                         order by seq desc limit 1
                      ''', key, commit_seq).fetchone()

            key = req_key
            version, value = row if row else (0, b'')
        # key is a sequence number
        else:
            row = sql('''select key, value from kv
                         where seq = ? and seq <= ? and lock is null
                      ''', key, commit_seq).fetchone()

            version = key
            key, value = row if row else ('', b'')

        sql.rollback()

        mime = mimetypes.guess_type(key)[0]

        write_http(writer, '200 OK', value, {
            'KVLOG_KEY': key,
            'KVLOG_VERSION': version,
            'KVLOG_COMMITTED': commit_seq,
            'Content-Type': mime if mime else 'application/octet-stream'})

        log('%s key(%d) version(%d) bytes(%d)', log_prefix, key, version,
            len(value))

        return writer.close()

    # SYNC - That's why this project exists
    if 'sync' != method:
        return writer.close()

    def log_prefix():
        i = 1 if sockname[0] == peername[0] else 0
        return 'master({}) slave({}) state({})'.format(
            sockname[i], peername[i], state['role'])

    try:
        # Reject any stray peer
        if args.token != await reader.readexactly(16):
            log('%s checksum mismatch', log_prefix())
            return writer.close()

        peer_term, peer_seq = struct.unpack(
            '!QQ', await reader.readexactly(16))

        log('%s term(%d) seq(%d)', log_prefix(), peer_term, peer_seq)

        my_term, my_seq = term_and_seq(sql)
        sql.rollback()

        # Reject a follower if my state is worse than peer's state
        if (my_term, my_seq) < (peer_term, peer_seq):
            log('%s rejected as (%d %d) < (%d %d)', log_prefix(),
                my_term, my_seq, peer_term, peer_seq)
            return writer.close()

        # Decided to accept this follower, lets calculate common max seq
        # Following is always correct, though we are sending data that a
        # follower might already have. But simple is better.
        #
        # Note that we always send at least one record, even if follower has
        # exactly the same data as leader
        #
        # my_term is always >= peer_term, never <, as ensure by previous check
        next_seq = peer_term if my_term > peer_term else peer_seq

        # Starting seq for replication is decided for this peer
        # But wait till we have a quorum of followers, or
        # sync() starts following someone better than us
        state['followers'][peername] = 0
        while 'voter' == state['role'] and has_quorum() is False:
            await asyncio.sleep(0.05)

        # Reject as we started following someone else
        if 'follower' == state['role']:
            state['followers'].pop(peername)
            log('%s rejected%s', log_prefix(), peername)
            return writer.close()

        # Leader election - step 1 of 3
        # Have a quorum and not following anyone else
        if 'voter' == state['role']:
            state['role'] = 'quorum'
            log('%s term(%d)', log_prefix(), my_seq+1)

        # Signal to the peer that it has been accepted as a follower
        writer.write(struct.pack('!Q', 0))

        # And start sending the data
        while True:
            rows = sql('''select seq, lock, key, value, uuid from kv
                          where seq >= ? order by seq
                       ''', next_seq).fetchall()

            if rows:
                # Send the current term/seq and uuid of the last row
                x, y = term_and_seq(sql)
                s = sql('select max(seq) from kv where seq<?',
                        next_seq).fetchone()[0]
                z = sql('select uuid from kv where seq=?',
                        s).fetchone()[0].encode()
                sql.rollback()

                writer.write(struct.pack('!QQQQ', x, y, s, len(z)))
                writer.write(z)

                # Send the rows to be replicated
                row_bytes = 0
                for seq, lock, key, val, uuid in rows:
                    row_bytes += send_row(writer, seq, lock, key, val, uuid)
            else:
                sql.rollback()

                # Avoid a busy loop
                await asyncio.sleep(0.05)
                continue

            panic(state['role'] not in ('quorum', 'candidate', 'leader'),
                  '{} not quorum/candidate/leader'.format(state['role']))

            writer.write(struct.pack('!Q', 0))

            peer_term, peer_seq = struct.unpack(
                '!QQ', await reader.readexactly(16))

            state['followers'][peername] = peer_seq

            next_seq = peer_seq + 1

            # Leader election - step 2 of 3
            # Wait for a quorum to fully replicate its log. Then insert a row
            # with null key to declare its candidature for leadership. The
            # seq for this row is the term number during new leaders reign.
            if 'quorum' == state['role']:
                max_seq = sql('select max(seq) from kv').fetchone()[0]

                # Quorum is in sync with its logs
                if max_seq == committed_seq():
                    sql('insert into kv values(?,null,null,null,?)', max_seq+1,
                        str((sockname, max_seq+1, int(time.time()*10**9),
                             os.getpid())))

                    sql.commit()
                    state['role'] = 'candidate'
                    log('%s term(%d)', log_prefix(), max_seq+1)
                    continue

                sql.rollback()

            # Leader election - step 3 of 3
            # Wait for a quorum to fully replicate its log, including the row
            # with null key, declaring the candidature of the new leader.
            # Since a quorum has accepted this new row and the logs are in sync
            # There is no backing off. Its safe to accept update requests.
            #
            # Leader is elected for the new term. WooHoo.
            if 'candidate' == state['role']:
                max_seq = sql('select max(seq) from kv').fetchone()[0]

                # Quorum is in sync with its logs
                if max_seq == committed_seq():
                    state['role'] = 'leader'
                    log('%s term(%d)', log_prefix(), max_seq)

                sql.rollback()

            log('%s term(%d) seq(%d) rows(%d) bytes(%d)',
                log_prefix(), peer_term, peer_seq, len(rows), row_bytes)
    except Exception:
        sql.rollback()
        state['followers'].pop(peername)
        log('%s quorum(%s) rejected(%d)', log_prefix(), has_quorum(),
            len(state['followers']))

        # Had a quorum once but not anymore. Too many complex cases to handle.
        # Its safer to start from a clean slate.
        if state['role'] in ('quorum', 'candidate', 'leader'):
            panic(has_quorum() is False, 'quorum lost')

    sql.rollback()
    writer.close()


def send_row(writer, seq, lock, key, value, uuid):
    writer.write(struct.pack('!Q', seq))

    lock = lock.encode() if lock else b''
    writer.write(struct.pack('!Q', len(lock)))
    writer.write(lock)

    key = key.encode() if key else b''
    writer.write(struct.pack('!Q', len(key)))
    writer.write(key)

    value = value if value else b''
    writer.write(struct.pack('!Q', len(value)))
    writer.write(value)

    uuid = uuid.encode() if uuid else b''
    writer.write(struct.pack('!Q', len(uuid)))
    writer.write(uuid)

    return 40 + len(lock) + len(key) + len(value) + len(uuid)


async def sync():
    sql = SQLite(args.db)
    state = args.state

    while True:
        for ip, port in args.peers:
            try:
                reader, writer = await asyncio.open_connection(ip, port)

                peername = writer.get_extra_info('peername')
                sockname = writer.get_extra_info('sockname')
            except Exception:
                reader = None
                continue

            i = 1 if sockname[0] == peername[0] else 0
            log_prefix = 'slave({}) master({})'.format(
                sockname[i], peername[i])

            try:
                writer.write(b'SYNC / HTTP/1.1\n\n')

                # Tell the leader our current state.
                # Leader would decide the starting point of replication.
                term, seq = term_and_seq(sql)
                sql.rollback()

                writer.write(args.token)
                writer.write(struct.pack('!QQ', term, seq))

                log('%s term(%d) seq(%d)', log_prefix, term, seq)

                await reader.readexactly(8)
                break
            except Exception:
                reader = None
                writer.close()

        if reader:
            break

        await asyncio.sleep(0.1)

    # We are already on our way to become a leader
    if 'voter' != state['role']:
        return writer.close()

    # We are a follower. Do not try to become a leader now
    state['role'] = 'follower'
    log('%s state(follower)', log_prefix)

    while True:
        row_count = 0

        x, y, s, z = struct.unpack('!QQQQ', await reader.readexactly(32))
        z = await reader.readexactly(z)

        row_bytes = 32 + len(z)

        xx, yy = term_and_seq(sql)
        zz = sql('select uuid from kv where seq=?', s).fetchone()[0].encode()

        panic(zz != z, '{} != {}'.format(zz, z))
        panic((xx, yy) > (x, y), '{} > {}'.format((xx, yy), (x, y)))

        while True:
            seq = struct.unpack('!Q', await reader.readexactly(8))[0]

            if 0 == seq:
                break

            lock = await reader.readexactly(
                struct.unpack('!Q', await reader.readexactly(8))[0])

            key = await reader.readexactly(
                struct.unpack('!Q', await reader.readexactly(8))[0])

            value = await reader.readexactly(
                struct.unpack('!Q', await reader.readexactly(8))[0])

            uuid = await reader.readexactly(
                struct.unpack('!Q', await reader.readexactly(8))[0])

            # Delete any redundant entries a crashed leader might have.
            sql('delete from kv where seq >= ?', seq)

            # Replicate the entry we got from the leader
            sql('insert into kv values(?,?,?,?,?)', seq,
                lock.decode() if lock else None,
                key.decode() if key else None,
                value if value else None,
                uuid.decode() if uuid else None)

            row_count += 1
            row_bytes += 40 + len(lock) + len(key) + len(value) + len(uuid)

        term, seq = term_and_seq(sql)
        sql.commit()

        log('%s term(%d) seq(%d) rows(%d) bytes(%d)',
            log_prefix, term, seq, row_count, row_bytes)

        # Inform the leader that we committed till seq and ask for more
        writer.write(struct.pack('!QQ', term, seq))


async def timekeeper():
    t = time.time()
    sec = int(time.time()*10**9 % 600)
    await asyncio.sleep(sec)
    panic(True, 'timeout({}) elapsed({})'.format(sec, int(time.time()-t)))


def server():
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

    sql = SQLite(args.db)

    # Rows are never updated. seq would increment with each insert.
    #
    # Null key indicates a new leader is taking over and start of a new term.
    # All the values till next null key are coordinated by this leader.
    # It is mandatory for a new leader to insert a new row with a null key to
    # establish its leaership. seq at this null key is the term number.
    #
    # There would be multiple rows with the same key. The row with the max
    # seq less than the committed sequence is the value returned by read.
    #
    # Committed sequence number is the max seq reported by a quorum.
    sql('''create table if not exists kv(
           seq   integer primary key autoincrement,
           lock  text,
           key   text,
           value blob,
           uuid  text)''')
    sql('create index if not exists key on kv(key)')

    # seq = 1, must always be present for initial election to start
    # It is utilized for storing configuration data for this db
    sql('delete from kv where seq < 2')
    sql('insert into kv values(0, null, null, null, "0")')
    sql('insert into kv values(1, null, null, null, "1")')

    rows = sql('''select seq from kv where key is null
                  order by seq desc limit 2''').fetchall()

    rows = sql('select key, seq from kv where ? < seq < ?',
               rows[1][0], rows[0][0])

    for key, seq in rows:
        sql('delete from kv where key = ? and seq < ?', key, seq)

    sql.commit()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.start_server(handler, '', args.port))

    args.state = dict(txns=dict(), role='voter', followers=dict())

    asyncio.ensure_future(sync())
    asyncio.ensure_future(timekeeper())

    def exception_handler(loop, context):
        panic(True, 'exception : {}'.format(context['future']))

    loop.set_exception_handler(exception_handler)
    loop.run_forever()


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


if __name__ == '__main__':
    # openssl req -x509 -nodes -subj / -sha256 --keyout ssl.key --out ssl.cert

    args = argparse.ArgumentParser()
    args.add_argument('--port', dest='port', type=int)
    args.add_argument('--peers', dest='peers')
    args.add_argument('--token', dest='token',
                      default=os.getenv('KVLOG_TOKEN', 'kvlog'))

    args.add_argument('--db', dest='db', default='kvlog')
    args.add_argument('--password', dest='password')

    args.add_argument('--key', dest='key')
    args.add_argument('--file', dest='file')
    args.add_argument('--value', dest='value')
    args = args.parse_args()

    args.token = hashlib.md5(args.token.encode()).digest()

    if args.peers:
        args.peers = sorted([(ip.strip(), int(port)) for ip, port in [
            p.split(':') for p in args.peers.split(',')]])

    client = Client(args.peers)

    if args.port:
        server()
    elif args.value:
        pprint.pprint(client.put(args.key, args.value))
    elif args.file:
        pprint.pprint(client.put(args.key, open(args.file, 'rb').read()))
    elif args.key:
        pprint.pprint(client.get(args.key))
    else:
        for k, v in client.state().items():
            print('{} : ({}, {}, {}, {})'.format(k, v['term'], v['seq'],
                  v['committed'], v['role']))
