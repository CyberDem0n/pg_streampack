pg_streampack
=============

`pg_streampack` is a PostgreSQL module that compresses replication streams
using lz4 before sending them over the network.
It reduces bandwidth usage between primary and standby servers, which is
especially useful over slow or costly connections.

Features
--------

- Transparent lz4 compression for replication streams
- Uses streaming (dictionary) LZ4 compression to obtain better compression ratios across sequential WAL traffic
- Minimal configuration changes for existing replication setups
- Works with streaming replication (physical and logical)
- Unmodified clients that do **not** request compression continue to operate normally

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

Note. Module must be installed and added to `shared_preload_libraries` on both servers.

Compatibility
-------------

- PostgreSQL 13+, however when building for v13 it needs to be linked with `liblz4`
- Supports both physical and logical streaming replication

Negotiation
-----------

`pg_streampack` only compresses replication stream messages when both the server and the client explicitly agree.
There is no new network handshake or protocol command; instead, negotiation happens by sending `-c pg_streampack.requested=on` via connection `options` if `pg_streampack.enabled` GUC is set.
If either side doesn’t “opt in”, replication messages stay exactly as PostgreSQL normally sends them.
The receiver detects compression by inspecting a flag bit in the `XLogData` header.

On-the-Wire Protocol Format Changes
-----------------------------------

Baseline PostgreSQL `XLogData` message structure (unchanged in size when uncompressed):

```
Offset  Size  Field
0       1     messageType = 'w'
1       8     walStart (uint64)
9       8     walEnd   (uint64)
17      8     sendTime (uint64)
25      ...   WAL payload (raw bytes)
```

When compressed by `pg_streampack`, the following modifications occur:

1. The *most significant bit* of the first byte of the network‑order `sendTime` field (i.e., header byte at offset 17) is set to 1.
2. A 4‑byte big-endian unsigned integer containing the **uncompressed payload length** is inserted immediately after the original 25‑byte header.
3. The WAL payload is replaced by compressed bytes of the original payload.

Resulting compressed layout:

```
Offset  Size  Field
0       1     'w'
1       8     walStart
9       8     walEnd
17      8     sendTime with most significant bit set (compression flag) *
25      4     uncompressed length (uint32)  (currently lower 30 bits used; top 2 reserved for different compression algorithms)
29      ...   compressed payload
```

\* On the receiver side this bit is cleared before the upper replication layers see the header, restoring expected semantics.

Observability
-------------

`pg_streampack` logs summary stats on shutdown:

- `total`: Sum of original message sizes (compressed + uncompressed paths).
- `uncompressed`: Bytes sent uncompressed.
- `compressed`: Bytes actually transmitted when compressed.
- `increased`: Count of compressed messages whose final size ≥ original.
