# db-rs

A Rust implementation of a Redis client and server.

![icon](./assets/icon.svg)

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
- [ ] leader/follower async replication
- [ ] resume from persisted offset
- [ ] read-your-writes and monotonic reads
- [ ] WATCH/CAS-style optimistic writes
- [ ] event-time windows and late corrections
