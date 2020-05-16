import os
import json
import time
import struct
import sqlite3
import asyncio
import logging
import hashlib
import argparse
import mimetypes
import urllib.parse
import urllib.request
from logging import critical as log


def append_rows(rows):
    conn = sqlite3.connect(args.db)

    for seq, lock, key, value, uuid in rows:
        # Delete any redundant entries a crashed leader might have.
        conn.execute('delete from kv where seq >= ?', (seq,))

        # Replicate the entry we got from the leader
        conn.execute('insert into kv values(?,?,?,?,?)', (seq,
                     lock if lock else None,
                     key if key else None,
                     value if value else None,
                     uuid if uuid else None))

    state['seq'] = conn.execute('select max(seq) from kv').fetchone()[0]
    state['term'] = conn.execute('''select seq from kv
                                    where key is null
                                    order by seq desc
                                    limit 1
                                 ''').fetchone()[0]

    conn.commit()
    conn.close()


def read_rows(seq, count=10000):
    conn = sqlite3.connect(args.db)
    rows = conn.execute('''select seq, lock, key, value, uuid from kv
                           where seq >= ? order by seq limit ?
                        ''', (seq, count)).fetchall()
    conn.close()
    return rows


def read_key(key, max_seq):
    conn = sqlite3.connect(args.db)
    row = conn.execute('''select seq, lock, value from kv
                          where key=? and seq <= ?
                          order by seq desc limit 1
                       ''', (key, max_seq)).fetchone()
    conn.close()
    return row


async def read_http(reader):
    first = await reader.readline()

    headers = dict()
    while True:
        line = await reader.readline()
        if not line.strip():
            break
        line = line.decode().split(':', 1)
        headers[line[0].strip().lower()] = line[1].strip()

    content = await reader.read(int(headers.get('content-length', '0')))

    return urllib.parse.unquote(first.decode()), headers, content


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
    #
    # This number + leader is a majority

    return int((len(args.peers) + 1) / 2)


def has_quorum():
    # We are good to go if we already got sync requests from a quorum or more
    return len(state['followers']) >= quorum()


def committed_seq():
    # It is pointless till we have a quorum
    if not has_quorum():
        return 0

    # If this node is a leader, it's max(seq) is guranteed to be biggest
    commits = sorted(list(state['followers'].values()), reverse=True)

    return commits[quorum() - 1]


def panic(cond, msg):
    if cond:
        log('panic(%s)', msg)
        os._exit(1)


async def handler(reader, writer):
    request, headers, req_content = await read_http(reader)
    method, url, _ = request.split()

    url = [u for u in url.split('/') if u]
    method = method.lower()

    url.extend([None, None, None])
    req_key, req_version, req_lock = url[0:3]

    req_key = req_key if req_key else None
    req_version = int(req_version) if req_version else 0
    if req_key and req_key.isdigit():
        req_key = int(req_key)

    # Current leader for each db managed by this cluster
    if 'get' == method and req_key is None:
        write_http(writer, '200 OK', dict(
            role=state['role'], term=state['term'], seq=state['seq'],
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

        # Let's batch requests till all log entries get replicated
        await state['append_flag'].wait()

        # No one else has processed this batch so far
        if type(state['txns'][peername]) is tuple:
            seq = state['seq']
            rows = list()

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
                            status='conflict', version=seq)
                        continue

                seq += 1
                rows.append((seq, None, key, value, str((
                    sockname, seq, int(time.time()*10**9), os.getpid()))))
                state['txns'][sock] = dict(status='fatal', version=seq)

            append_rows(rows)
            state['append_flag'].clear()
            state['replicate_flag'].set()

            for sock in state['txns']:
                if type(state['txns'][sock]) is dict:
                    if 'fatal' == state['txns'][sock]['status']:
                        state['txns'][sock]['status'] = 'ok'

            panic('leader' != state['role'],
                  "{} can't commit".format(state['role']))

        # Someone has committed this batch by now. Lets examine the result.
        result = state['txns'].pop(peername)

        # Great - our request was not rejected due to a conflict.
        # Let's wait till a quorum replicates the data in their logs
        if 'ok' == result['status']:
            while result['version'] > committed_seq():
                state['append_flag'].clear()
                await state['append_flag'].wait()

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

        row = read_key(req_key, commit_seq)
        seq, lock, value = row if row else (0, '', b'')

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

    if 'token' == method:
        msg = await reader.readexactly(
            struct.unpack('!Q', await reader.readexactly(8))[0])
        ip, port = msg.decode().split(':')

        state['tokens'][(ip, int(port))] = await reader.readexactly(16)

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
        if state['token'] != await reader.readexactly(16):
            log('%s token mismatch', log_prefix())
            return writer.close()

        peer_term, peer_seq = struct.unpack(
            '!QQ', await reader.readexactly(16))

        log('%s term(%d) seq(%d)', log_prefix(), peer_term, peer_seq)

        # Reject a follower if my state is worse than peer's state
        if (state['term'], state['seq']) < (peer_term, peer_seq):
            log('%s rejected as (%d %d) < (%d %d)', log_prefix(),
                state['term'], state['seq'], peer_term, peer_seq)
            return writer.close()

        # Decided to accept this follower, lets calculate common max seq
        # Following is always correct, though we are sending data that a
        # follower might already have. But simple is better.
        #
        # Note that we always send at least one record, even if follower has
        # exactly the same data as leader
        #
        # my_term is always >= peer_term, never <, as ensure by previous check
        next_seq = peer_term if state['term'] > peer_term else peer_seq

        # Starting seq for replication is decided for this peer
        # But wait till we have a quorum of followers, or
        # sync() starts following someone better than us
        state['followers'][peername] = 0
        while 'voter' == state['role'] and has_quorum() is False:
            await asyncio.sleep(1)

        # Reject as we started following someone else
        if 'follower' == state['role']:
            state['followers'].pop(peername)
            log('%s rejected%s as a follower', log_prefix(), peername)
            return writer.close()

        # Leader election - step 1 of 3
        # Have a quorum and not following anyone else
        if 'voter' == state['role']:
            state['role'] = 'quorum'
            log('%s term(%d)', log_prefix(), state['seq']+1)

        # Signal to the peer that it has been accepted as a follower
        writer.write(struct.pack('!Q', 0))

        # And start sending the data
        while True:
            rows = read_rows(next_seq)
            row_bytes = 0

            for seq, lock, key, value, uuid in rows:
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

                row_bytes += 40 + len(lock) + len(key) + len(value) + len(uuid)

            if not rows:
                state['append_flag'].set()
                state['replicate_flag'].clear()

                # Avoid a busy loop
                await state['replicate_flag'].wait()
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
                # Quorum is in sync with its logs
                if state['seq'] == committed_seq():
                    new_term = state['seq'] + 1
                    append_rows([(new_term, None, None, None,
                                  str((sockname, new_term,
                                       int(time.time()*10**9), os.getpid())))])

                    state['role'] = 'candidate'
                    log('%s term(%d)', log_prefix(), new_term)
                    continue

            # Leader election - step 3 of 3
            # Wait for a quorum to fully replicate its log, including the row
            # with null key, declaring the candidature of the new leader.
            # Since a quorum has accepted this new row and the logs are in sync
            # There is no backing off. Its safe to accept update requests.
            #
            # Leader is elected for the new term. WooHoo.
            if 'candidate' == state['role']:
                # Quorum is in sync with its logs
                if state['seq'] == committed_seq():
                    state['role'] = 'leader'
                    log('%s term(%s)', log_prefix(), state['term'])

            log('%s term(%d) seq(%d) rows(%d) bytes(%d)',
                log_prefix(), peer_term, peer_seq, len(rows), row_bytes)
    except Exception as e:
        state['followers'].pop(peername)
        log('%s exception(%s)', log_prefix(), e)

        # Had a quorum once but not anymore. Too many complex cases to handle.
        # Its safer to start from a clean slate.
        if state['role'] in ('quorum', 'candidate', 'leader'):
            panic(has_quorum() is False, 'quorum lost')

    writer.close()


async def sync():
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
                writer.write(state['tokens'].get((ip, port), state['token']))
                writer.write(struct.pack('!QQ', state['term'], state['seq']))

                log('%s term(%d) seq(%d)', log_prefix,
                    state['term'], state['seq'])

                await reader.readexactly(8)
                break
            except Exception:
                reader = None
                writer.close()

        if reader:
            break

        await asyncio.sleep(1)

    # We are already on our way to become a leader
    if 'voter' != state['role']:
        return writer.close()

    # We are a follower. Do not try to become a leader now
    state['role'] = 'follower'
    log('%s state(follower)', log_prefix)

    while True:
        rows = list()
        row_bytes = 0

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

            row_bytes += 40 + len(lock) + len(key) + len(value) + len(uuid)

            rows.append((seq,
                         lock.decode() if lock else None,
                         key.decode() if key else None,
                         value,
                         uuid.decode()))

        append_rows(rows)

        log('%s term(%d) seq(%d) rows(%d) bytes(%d)',
            log_prefix, state['term'], state['seq'], len(rows), row_bytes)

        # Inform the leader that we committed till seq and ask for more
        writer.write(struct.pack('!QQ', state['term'], state['seq']))


async def timekeeper():
    t, sec = time.time(), int(time.time()*10**9 % 10)

    while time.time() < t + sec:
        for ip, port in args.peers:
            try:
                reader, writer = await asyncio.open_connection(ip, port)

                my_ip = writer.get_extra_info('sockname')[0]
                msg = '{}:{}'.format(my_ip, args.port).encode()

                writer.write(b'TOKEN / HTTP/1.1\n\n')
                writer.write(struct.pack('!Q', len(msg)))
                writer.write(msg)
                writer.write(state['token'])
                writer.close()
            except Exception:
                pass

        await asyncio.sleep(1)

    panic(True, 'timeout({}) elapsed({})'.format(sec, int(time.time()-t)))


def server():
    logging.basicConfig(format='%(asctime)s %(process)d : %(message)s')

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
    conn = sqlite3.connect(args.db)
    conn.execute('''create table if not exists kv(
                    seq   integer primary key autoincrement,
                    lock  text,
                    key   text,
                    value blob,
                    uuid  text)''')
    conn.execute('create index if not exists key on kv(key)')

    # seq = 1, must always be present for initial election to start
    # It is utilized for storing configuration data for this db
    conn.execute('delete from kv where seq < 2')
    conn.execute('insert into kv values(0, null, null, null, "0")')
    conn.execute('insert into kv values(1, null, null, null, "1")')

    rows = conn.execute('''select seq from kv where key is null
                           order by seq desc limit 2''').fetchall()

    rows = conn.execute('select key, seq from kv where ? < seq < ?',
                        (rows[1][0], rows[0][0]))

    for key, seq in rows:
        conn.execute('delete from kv where key = ? and seq < ?', (key, seq))

    conn.commit()
    conn.close()

    append_rows([])

    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.start_server(handler, '', args.port))

    asyncio.ensure_future(sync())
    asyncio.ensure_future(timekeeper())

    def exception_handler(loop, context):
        panic(True, 'exception : {}'.format(context['future']))

    loop.set_exception_handler(exception_handler)
    loop.run_forever()


if __name__ == '__main__':
    # openssl req -x509 -nodes -subj / -sha256 --keyout ssl.key --out ssl.cert

    args = argparse.ArgumentParser()
    args.add_argument('--peers', dest='peers')
    args.add_argument('--port', dest='port', type=int)
    args.add_argument('--db', dest='db', default='kvlog.sqlite3')
    args = args.parse_args()

    args.peers = sorted([(ip.strip(), int(port)) for ip, port in [
        p.split(':') for p in args.peers.split(',')]])

    state = dict(
        txns=dict(),
        role='voter',
        followers=dict(),
        token=hashlib.md5(str(int(time.time()*10**9)).encode()).digest(),
        tokens=dict(),
        append_flag=asyncio.Event(),
        replicate_flag=asyncio.Event())

    server()
