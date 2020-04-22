import os
import json
import glob
import time
import pprint
import signal
import struct
import sqlite3
import asyncio
import hashlib
import logging
import argparse
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


def write_http(writer, status, obj=None):
    writer.write('HTTP/1.0 {}\n\n'.format(status).encode())

    if type(obj) is bytes:
        content = obj
    elif obj:
        content = json.dumps(obj, indent=4, sort_keys=True).encode()
    else:
        content = status.encode()

    writer.write(content)


def quorum(state):
    # Peers are the servers, other than this, in the cluster.
    # To be a leader, we want at least half of the peers to follow.
    # 1 peers -> 1 follower,  2 peers -> 1 follower
    # 3 peers -> 2 followers, 4 peers -> 2 followers

    # This number + leader is a majority
    return int((len(args.peers) + 1) / 2)


def has_quorum(state):
    # We are good to go if we already got sync requests from a quorum or more
    return len(state['followers']) >= quorum(state)


def committed_seq(state):
    # It is pointless till we have a quorum
    if not has_quorum(state):
        return 0

    # If this node is a leader, it's max(seq) is guranteed to be biggest
    commits = sorted(list(state['followers'].values()), reverse=True)

    return commits[quorum(state) - 1]


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


async def handler(reader, writer):
    request, headers = await read_http(reader)
    method, url, _ = request.split()

    url = url.strip('/')

    # Current leader for each db managed by this cluster
    if not url:
        write_http(writer, '200 OK', {
            k: committed_seq(args.state[k])
            for k, v in args.state.items() if 'leader' == v['role']})

        return writer.close()

    url = url.split('/')

    if url[0] not in args.state:
        write_http(writer, '404 Not Found', 'DB({}) not found'.format(url[0]))
        return writer.close()

    sql = SQLite(os.path.join('db', url[0]))
    state = args.state[url[0]]

    peername = writer.get_extra_info('peername')
    sockname = writer.get_extra_info('sockname')

    # Don't read/write until elected leader or lost quorum
    if method.lower() in ('get', 'put', 'post'):
        no_quorum = not has_quorum(state)
        not_a_leader = 'leader' != state['role']
        is_a_follower = 'following' in state

        if not_a_leader or is_a_follower or no_quorum:
            write_http(writer, '503 Service Unavailable', state)
            return writer.close()

    # APPEND
    if method.lower() in ('put', 'post'):
        content = await reader.read(int(headers['content-length']))
        state['txns'][sockname] = json.loads(content.decode())

        # Lets wait for enough requests so that we can commit a batch
        await asyncio.sleep(0.1)

        # No one else has processed this batch in last one second
        if type(state['txns'][sockname]) is list:
            # Invalid and conflicting keys would be rejected
            keyset = set([None, ''])

            # Check for conflicting requests, to be rejected
            for sock in state['txns']:
                if type(state['txns'][sock]) is not list:
                    continue

                for key, version, value in state['txns'][sock]:
                    conflict = None

                    # Another requests already proposed an update to thie key
                    if key in keyset:
                        conflict = dict(status='conflict',
                                        key=key, seq=version)

                    # A request with optimistic locking - check if all ok
                    elif key and version:
                        seq = sql('''select max(seq) from kv where key=?
                                  ''', key.encode()).fetchone()[0]

                        # Conflict as version already updated
                        if version != seq:
                            conflict = dict(status='mismatch',
                                            key=key, seq=version,
                                            existing_seq=seq)

                    if conflict:
                        state['txns'][sock] = conflict
                        break

                    # Conflict free so far. To be rejected if seen again
                    if key:
                        keyset.add(key)

            # Only non conflicting keys remaining. Let's append them to log
            for sock in state['txns']:
                if type(state['txns'][sock]) is not list:
                    continue

                for key, version, value in state['txns'][sock]:
                    sql('insert into kv values(null,?,?)',
                        key.encode(),
                        json.dumps(value).encode())

                state['txns'][sock] = dict(status='ok',
                                           keys=len(state['txns'][sock]))

            sql.commit()

            term, seq = term_and_seq(sql)
            sql.rollback()

            for sock in state['txns']:
                state['txns'][sock]['txn_seq'] = seq

        # Someone has committed this batch by now. Lets examine the result.
        result = state['txns'].pop(sockname)

        # Great - our request was not rejected due to a conflict.
        # Let's wait till a quorum replicates the data in their logs
        if 'ok' == result['status']:
            while result['txn_seq'] < committed_seq(state):
                await asyncio.sleep(0.1)

        result['committed_seq'] = committed_seq(state)

        write_http(writer, '200 OK', result)
        log('put(%s) content(%s)', url[0], str(result))

        return writer.close()

    # READ
    if 'get' == method.lower():
        cmd = url[1]

        if 'keys' == cmd:
            keys = [k.strip() for k in url[2].split(',')]

        if cmd in ('key', 'static'):
            keys = [url[2].strip()]

        # Must not return data that is not replicated by a quorum
        commit_seq = committed_seq(state)

        result = list()
        for key in keys:
            row = sql('''select seq, value from kv
                         where key=? and seq <= ?
                         order by seq desc limit 1
                      ''', key.encode(), commit_seq).fetchone()
            if row:
                result.append((key, row[0], json.loads(row[1].decode())))

        sql.rollback()

        if 'keys' == cmd:
            write_http(writer, '200 OK', result)

        if cmd in ('key', 'static'):
            write_http(writer, '200 OK', result[0][2] if result else b'')

        log('get(%s) keys(%d) commit_seq(%d)', url[0], len(result), commit_seq)
        return writer.close()

    assert(method.lower() in ('sync'))

    def log_prefix():
        i = 1 if '127.0.0.1' == sockname[0] else 0
        return 'master({}) slave({}) role({}) db({})'.format(
            sockname[i], peername[i], state['role'], url[0])

    # SYNC - That's why this project exists
    try:
        # Reject any stray peer
        if args.token != await reader.readexactly(16):
            log('%s checksum mismatch', log_prefix())
            return writer.close()

        peer_term, peer_seq = struct.unpack(
            '!QQ', await reader.readexactly(16))
        peer_chksum = await reader.readexactly(16)

        log('%s term(%d) seq(%d)', log_prefix(), peer_term, peer_seq)

        my_term, my_seq = term_and_seq(sql)
        sql.rollback()

        # If quorum already reached, accept more followers, after sanity check
        if has_quorum(state):
            if (my_term, my_seq) < (peer_term, peer_seq):
                log('%s rejected as (%d %d) < (%d %d)', log_prefix(),
                    my_term, my_seq, peer_term, peer_seq)
                return writer.close()

        # else accept only if my (term, seq, uniq_id) is bigger than peer
        else:
            mystate = (my_term, my_seq, state['chksum'])
            peerstate = (peer_term, peer_seq, peer_chksum)

            # These two can never be equal due to unique checksum
            if mystate < peerstate:
                log('%s rejected as (%d %d id) < (%d %d id)', log_prefix(),
                    my_term, my_seq, peer_term, peer_seq)
                return writer.close()

        # Decided to accept this follower, lets calculate common max seq
        # This is always correct. Even though inefficient.
        # When a peer is a couple of terms behind, its ok if it gets a little
        # redundant data, as it is not a common case.
        next_seq = peer_term

        # But every time, a quorum would have same last term, after the current
        # leader crashes or exits. Let's try to optimize this frequent case
        # by not sending all the data from the last term again.
        # We know that an entry can not be present in the log if the previous
        # one was incorrect, lets just start from the last entry the peer has
        # Only one redundant row in this case, but still avoids lots of complex
        # case we need to consider otherwise.
        if my_term == peer_term:
            next_seq = peer_seq

        # Starting seq for replication is decided for this peer
        # But wait till we have a quorum of followers, or
        # sync() starts following someone better than us
        state['followers'][peername] = 0
        while has_quorum(state) is False and 'following' not in state:
            await asyncio.sleep(0.1)

        # Reject this follower if we started following someone else
        if 'following' in state:
            log('%s rejected following(%s)', log_prefix(), state['following'])
            return writer.close()

        # Great. Have a quorum and not following anyone else
        # Set the flag so that sync() exits
        #
        # Leader election - step 1 of 3
        state['role'] = 'quorum'

        # Signal to the peer that it has been accepted as a follower
        writer.write(struct.pack('!Q', 0))

        # And start sending the data
        while True:
            cur = sql('''select seq, key, value from kv
                         where seq >= ? order by seq
                      ''', next_seq)

            rows_sent = 0
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

                rows_sent += 1

            sql.rollback()

            writer.write(struct.pack('!Q', 0))

            peer_term, peer_seq = struct.unpack(
                '!QQ', await reader.readexactly(16))

            state['followers'][peername] = peer_seq

            next_seq = peer_seq + 1

            # Wait for a quorum to fully replicate its log. Then insert a row
            # with null key to declare its candidature for leadership. The
            # seq for this row is the term number during new leaders reign.
            #
            # Leader election - step 2 of 3
            if 'quorum' == state['role']:
                max_seq = sql('select max(seq) from kv').fetchone()[0]

                # Quorum is in sync with its logs
                if max_seq == committed_seq(state):
                    sql('insert into kv values(null, null, ?)', '{}:{}'.format(
                        sockname, int(time.time()*10**9)).encode())

                    state['role'] = 'candidate'
                    sql.commit()
                    continue

                sql.rollback()

            # Wait for a quorum to fully replicate its log, including the row
            # with null key, declaring the candidature of the new leader.
            # Since a quorum has accepted this new row and the logs are in sync
            # There is no backing off. Its safe to accept update requests.
            #
            # Leader is elected for the new term. WooHoo.
            #
            # Leader election - step 3 of 3
            if 'candidate' == state['role']:
                max_seq = sql('select max(seq) from kv').fetchone()[0]

                # Quorum is in sync with its logs
                if max_seq == committed_seq(state):
                    state['role'] = 'leader'

                sql.rollback()

            if rows_sent > 0:
                log('%s term(%d) seq(%d) sent(%d)',
                    log_prefix(), peer_term, peer_seq, rows_sent)

            # Avoid a busy loop
            await asyncio.sleep(0.1)
    except Exception:
        sql.rollback()
        state['followers'].pop(peername, None)
        log('%s quorum(%s) rejected(%d)', log_prefix(), has_quorum(state),
            len(state['followers']))

        # Had a quorum once but not anymore. Too many complex cases to handle.
        # Its safer to exit and start from a clean slate.
        if 'voter' != state['role'] and has_quorum(state) is False:
            os._exit(1)

    sql.rollback()
    writer.close()


async def sync(db):
    sql = SQLite(os.path.join('db', db))
    state = args.state[db]

    while True:
        # Got a quorum of followers, no need to follow anyone else
        if 'voter' != state['role']:
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

                # Tell the leader our current state.
                # Leader would decide the starting point of replication for us.
                term, seq = term_and_seq(sql)

                writer.write(args.token)
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

        await asyncio.sleep(0.1)

    # Got a quorum to lead before our search for leader was successful
    if 'voter' != state['role']:
        return writer.close()

    # Ensure that we do not try to become a leader anymore
    # as we have found a better leader
    state['following'] = (ip, port)

    while True:
        rows_received = 0
        while True:
            seq = struct.unpack('!Q', await reader.readexactly(8))[0]

            if 0 == seq:
                break

            key = await reader.readexactly(
                struct.unpack('!Q', await reader.readexactly(8))[0])
            value = await reader.readexactly(
                struct.unpack('!Q', await reader.readexactly(8))[0])

            # Delete any redundant entries earlier leader might have.
            # This should be done only once - optimization - some day.
            sql('delete from kv where seq >= ?', seq)

            # Replicate the entry we got from the leader
            sql('insert into kv values(?,?,?)', seq,
                key if key else None, value if value else None)

            log('%s seq(%d) key(%d) value(%d)',
                log_prefix, seq, len(key), len(value))

            rows_received += 1

        sql.commit()

        term, seq = term_and_seq(sql)

        if rows_received:
            log('%s term(%d) seq(%d) received(%d)',
                log_prefix, term, seq, rows_received)

        # Inform the leader that we committed till seq and ask for more
        writer.write(struct.pack('!QQ', term, seq))


def server():
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')
    signal.alarm(args.timeout)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.start_server(handler, '', args.port))

    for db in sorted(glob.glob('db/*.sqlite3')):
        db = '.'.join(os.path.basename(db).split('.')[:-1])
        # sql = SQLite(os.path.join('db', db))
        state = args.state.setdefault(db, dict())

        # cluster = sql('select value from kv where seq=0').fetchone()[0]
        # sql.rollback()

        state['txns'] = dict()
        state['role'] = 'voter'
        state['followers'] = dict()
        state['chksum'] = hashlib.md5((db + str(args.peers)).encode()).digest()

        asyncio.ensure_future(sync(db))

    def exception_handler(loop, context):
        log(context['future'])
        os._exit(1)

    loop.set_exception_handler(exception_handler)
    loop.run_forever()


def init():
    if not os.path.isdir('db'):
        os.mkdir('db')

    sql = SQLite(os.path.join('db', args.db))

    # Simplest possible schema for a versioned key/value store
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
        key   blob,
        value blob)''')
    sql('create index if not exists key on kv(key)')

    # seq = 1, must always be present for initial election to start
    # It is utilized for storing configuration data for this db
    sql('delete from kv where seq=1')
    sql('insert into kv values(1, null, ?)', args.password)
    sql.commit()


class Client():
    def __init__(self, servers):
        self.servers = servers
        self.leaders = dict()

    def leader(self, db=None):
        if db not in self.leaders:
            result = dict()

            for ip, port in self.servers:
                url = 'http://{}:{}'.format(ip, port)

                try:
                    with urllib.request.urlopen(url) as r:
                        for k, v in json.loads(r.read()).items():
                            result[k] = dict(seq=v, url=url + '/{}'.format(k))
                except Exception:
                    pass

            self.leaders = result

        return self.leaders.get(db, None)

    def put(self, db, filename):
        with open(filename, 'rb') as f:
            data = f.read()

        for i in range(10):
            try:
                req = urllib.request.Request(
                    self.leader(db)['url'], data=data, method='PUT')

                with urllib.request.urlopen(req) as r:
                    return json.loads(r.read())
            except Exception:
                self.leaders = None
                time.sleep(2)

    def key(self, db, key):
        for i in range(10):
            try:
                url = '{}/key/{}'.format(self.leader(db)['url'], key)

                with urllib.request.urlopen(url) as r:
                    return r.read()
            except Exception:
                self.leaders = None
                time.sleep(2)

    def keys(self, db, keys):
        for i in range(10):
            try:
                url = '{}/keys/{}'.format(self.leader(db)['url'],
                                          ','.join(keys))
                with urllib.request.urlopen(url) as r:
                    return json.loads(r.read())
            except Exception:
                self.leaders = None
                time.sleep(2)


if __name__ == '__main__':
    # openssl req -x509 -nodes -subj / -sha256 --keyout ssl.key --out ssl.cert

    args = argparse.ArgumentParser()
    args.add_argument('--port', dest='port', type=int)
    args.add_argument('--peers', dest='peers')
    args.add_argument('--token', dest='token',
                      default=os.getenv('KEYVALUESTORE', 'keyvaluestore'))
    args.add_argument('--timeout', dest='timeout', type=int, default=60)

    args.add_argument('--init', dest='db')
    args.add_argument('--password', dest='password')

    args.add_argument('--key', dest='key')
    args.add_argument('--keys', dest='keys')
    args.add_argument('--put', dest='put')
    args = args.parse_args()

    args.state = dict()
    args.token = hashlib.md5(args.token.encode()).digest()
    args.timeout = int(time.time()*10**9) % min(args.timeout, 600)

    if args.peers:
        args.peers = sorted([(ip.strip(), int(port)) for ip, port in [
            p.split(':') for p in args.peers.split(',')]])

    client = Client(args.peers)

    if args.port:
        server()
    elif args.db:
        init()
    elif args.key:
        db, key = args.key.split('/')
        log('Fetching : {}'.format(client.leader(db)))

        print(client.key(db, key))
    elif args.keys:
        db, keys = args.keys.split('/')
        log('Fetching : {}'.format(client.leader(db)))

        for key, version, value in client.keys(db, keys.split(',')):
            print('{} {} {}'.format(key, version, value))
    elif args.put:
        db, filename = args.put.split('/')
        log('Updating : {}'.format(client.leader(db)))

        pprint.pprint(client.put(db, filename))
    else:
        client.leader()
        pprint.pprint(client.leaders)
