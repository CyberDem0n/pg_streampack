pg_streampack
=============

`pg_streampack` is a PostgreSQL module that compresses replication streams
using ZSTD or LZ4 before sending them over the network.
It reduces bandwidth usage between primary and standby servers, which is
especially useful over slow or costly connections.

Features
--------

- Transparent compression for replication streams
  - Supports both physical and logical streaming replication
- Uses streaming compression to obtain better compression ratios across sequential WAL traffic
- Minimal configuration changes for existing replication setups
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
    pg_streampack.compression = 'zstd,lz4'

    # compresses replication messages starting from 32 bytes
    pg_streampack.min_size = 32

Restart the servers after changing the configuration.

The following values are supported for `pg_streampack.compression`:

- `zstd` (requires PostgreSQL 15+ built with `--with-zstd`)
- `lz4`  (requires PostgreSQL 14+ built with `--with-lz4`)

Note. Module must be installed and added to `shared_preload_libraries` on both servers.

Compatibility
-------------

- Works with both physical and logical streaming replication
- By default supports the same compression algorithms that are supported by PostgreSQL major version:
	- PostgreSQL 14+ - LZ4 (if PostgreSQL is compiled with `--with-lz4`)
	- PostgreSQL 15+ - LZ4 and ZSTD (if PostgreSQL is compiled with `--with-lz4` and `--with-zstd`)
- Compiles with PostgreSQL 13, however by defaul will not do anything useful
	- It is possible to manually define `USE_LZ4` and `USE_ZSTD` and link with `-llz4` and `-lzstd`. It will enable "unsupported" algorithms on PostgreSQL older than 15.
- Supports both physical and logical streaming replication

Negotiation
-----------

`pg_streampack` only compresses replication stream messages when both sides explicitly agree.
There is no new network handshake or protocol command; instead, negotiation happens by sending `-c pg_streampack.requested=<algorithms>` via connection `options`.
The downstream (standby or logical subscriber) sends a list of supported algorithms (configured using `pg_streampack.compression` GUC) as a `pg_streampack.requested` GUC.
The upstream (primary or publisher) chooses the first algorithm from the list that it also has enabled in its own `pg_streampack.compression` list.
If no common algorithm is found, replication proceeds **uncompressed** (no error) with the original PostgreSQL replication messages format.
The receiver/subscriber detects compression by inspecting a flag bit in the `XLogData` header.

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
2. A 4‑byte big-endian unsigned integer containing compression algorithm (2 bits) and the **uncompressed payload length** (30 bits) is inserted immediately after the original 25‑byte header.
3. The WAL payload is replaced by compressed bytes of the original payload.

Resulting compressed layout:

```
Offset  Size  Field
0       1     'w'
1       8     walStart
9       8     walEnd
17      8     sendTime with most significant bit set (compression flag) *
25      4     (uint32), 2 bits for compression algorithm, 30 bits for actual uncompressed length
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
