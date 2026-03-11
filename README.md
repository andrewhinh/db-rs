# db-rs

A Rust implementation of a Redis client and server.

![icon](./assets/icon.svg)

Features include:

- PING, GET, SET, PUBLISH, SUBSCRIBE, DEL, EXISTS, EXPIRE, TTL, PTTL
- AOF append for mutating commands, replay on startup, and truncation-tolerant replay
- Snapshot and restore
- AOF rewrite from current state with tombstones
- Ordered change stream on `__changes__` via broadcast
- Snapshot and offset bootstrap (`__changes__@boot`) for new consumers
- Leader/follower async replication
- Resume from persisted offset
- Read-your-writes and monotonic reads
- CAS for optimistic concurrency
- Event-time tumbling windows with watermark and late-event corrections

## Development

### Installation

- [rustup](https://rustup.rs/)
- [prek](https://prek.j178.dev/installation/)

```bash
prek install
```

### Commands

Start the server and client examples:

```bash
mprocs
```

## Roadmap

- [x] port mini-redis
- [x] add DEL and EXISTS
- [x] add EXPIRE and TTL/PTTL
- [x] append AOF for mutating cmds
- [x] replay AOF on startup
- [x] snapshot and restore
- [x] AOF rewrite and tombstones
- [x] ordered change stream
- [x] snapshot and offset bootstrap
- [x] leader/follower async replication
- [x] resume from persisted offset
- [x] read-your-writes and monotonic reads
- [x] WATCH/CAS-style optimistic writes
- [x] event-time windows and late corrections

## Credit

- [mini-redis](https://github.com/tokio-rs/mini-redis)
- [Designing Data-Intensive Applications](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781098119058/)
