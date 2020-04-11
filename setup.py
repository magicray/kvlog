import glob
from distutils.core import setup

setup(
  name = 'keyvaluestore',
  modules = ['keyvaluestore'],
  version = '0.1',
  description = 'Synchronously replicated Key Value store',
  long_description = 'A synchronously replicated key value store built using sqlite for storage. Replication is raft-like while leader election is based on paxos. Go to https://github.com/magicray/keyvaluestore for details',
  author = 'Bhupendra Singh',
  author_email = 'bhsingh@gmail.com',
  url = 'https://github.com/magicray/keyvaluestore',
  keywords = ['paxos', 'raft', 'sqlite', 'replicated', 'distributed', 'key', 'value', 'synchronous', 'sync'],
  classifiers=[
    'Development Status :: 4 - Beta',
    'Intended Audience :: Developers',
    'Programming Language :: Python :: 3.7'
  ],
)
