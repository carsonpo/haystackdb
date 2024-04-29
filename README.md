# HaystackDB

> Minimal but performant Vector DB

## Features

- Binary embeddings by default (soon int8 reranking)
- JSON filtering for queries
- Scalable, distributed architecture for use with multi replica deployments
- Durable (WAL), persistent data, mem mapped for fast access in the client

## Benchmarks

> On a MacBook with an M2, 1024 dimension, binary quantized.

> FAISS is using a flat index, so brute force, but it's in memory. Haystack is storing the data on disk, and also brute forces.

TLDR is Haystack is ~10x faster despite being stored on disk.

```
100,000 Vectors
Haystack — 3.44ms
FAISS    — 29.67ms

500,000 Vectors
Haystack — 11.98ms
FAISS    - 146.50ms

1,000,000 Vectors
Haystack — 22.65ms
FAISS    — 293.91ms
```

## Roadmap

- **Quickstart Guide**
- **Quality benchmarks**
- Int8 reranking
- ~~Better queries with more than simple equality~~ (this is done now)
- Full text search
- ~~Better insertion performance with batch B+Tree insertion~~ (could probably be further improved, but good for now)
- Point in time backups/rollback
- Cursor based pagination
- Schema migrations
- Vector clustering for improved search perf
