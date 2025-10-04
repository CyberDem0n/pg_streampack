pg_streampack
=============

`pg_streampack` is a PostgreSQL module that compresses replication streams
using lz4 before sending them over the network.
It reduces bandwidth usage between primary and standby servers, which is
especially useful over slow or costly connections.

Features
--------

- Transparent lz4 compression for replication streams
- Minimal configuration changes for existing replication setups
- Works with streaming replication (physical and logical)

Installation
------------

Build and install:

    make USE_PGXS=1
    sudo make USE_PGXS=1 install

Configuration
-------------

Example setting in postgresql.conf:

    # Enable replication stream compression
    shared_preload_libraries = 'pg_streampack'
    pg_streampack.enabled = on

    # compresses replication messages starting from 32 bytes
    pg_streampack.min_size = 32

Restart the servers after changing the configuration.

Compatibility
-------------

- PostgreSQL 13+, however when building for v13 it needs to be linked with `liblz4`
- Supports both physical and logical streaming replication

Limitations and Known Problems
------------------------------

- Only lz4 is supported
- Module must be installed and added to `shared_preload_libraries` on both,
  primary and standby
- Slight increase in CPU usage due to compression/decompression
