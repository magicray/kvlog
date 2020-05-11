from distutils.core import setup

setup(
    name='kvlog',
    version='0.7',
    packages=['kvlog'],
    description='Synchronously replicated Key Value Log/Store',
    long_description='''
    A synchronously replicated log based key value store built using sqlite
    for storage. Replication is raft-like while leader election is based on
    paxos.

    Go to https://github.com/magicray/kvlog for details''',
    url='https://github.com/magicray/kvlog',
    author='Bhupendra Singh',
    author_email='bhsingh@gmail.com',
    keywords=['paxos', 'raft', 'sqlite', 'replicated', 'distributed', 'key',
              'value', 'synchronous', 'sync'])
