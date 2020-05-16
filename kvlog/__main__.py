import pprint
import argparse
from . import Client

# openssl req -x509 -nodes -subj / -sha256 --keyout ssl.key --out ssl.cert

args = argparse.ArgumentParser()
args.add_argument('--key', dest='key')
args.add_argument('--file', dest='file')
args.add_argument('--value', dest='value')
args.add_argument('--peers', dest='peers')
args = args.parse_args()

client = Client(sorted([(ip.strip(), int(port)) for ip, port in
                        [p.split(':') for p in args.peers.split(',')]]))

if args.value:
    pprint.pprint(client.put(args.key, args.value))
elif args.file:
    pprint.pprint(client.put(args.key, open(args.file, 'rb').read()))
elif args.key:
    pprint.pprint(client.get(args.key))
else:
    for k, v in client.state().items():
        print('{} : ({}, {}, {}, {})'.format(k, v['term'], v['seq'],
              v['committed'], v['role']))
