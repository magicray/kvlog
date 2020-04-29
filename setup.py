from distutils.core import setup

setup(
  name='kvlog',
  modules=['kvlog'],
  version='0.6',
  description='Synchronously replicated Key Value Log/Store',
  long_description='A synchronously replicated log based key value store built using sqlite for storage. Replication is raft-like while leader election is based on paxos. Go to https://github.com/magicray/kvlog for details',
  author='Bhupendra Singh',
  author_email='bhsingh@gmail.com',
  url='https://github.com/magicray/kvlog',
  keywords=['paxos', 'raft', 'sqlite', 'replicated', 'distributed', 'key', 'value', 'synchronous', 'sync'],
  classifiers=[
    'Development Status :: 4 - Beta',
    'Intended Audience :: Developers',
    'Programming Language :: Python :: 3.7'
  ],
)
