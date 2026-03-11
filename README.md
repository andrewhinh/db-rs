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
- [ ] add DEL and EXISTS
- [ ] add EXPIRE and TTL/PTTL
- [ ] append AOF for mutating cmds
- [ ] replay AOF on startup
- [ ] snapshot and restore
- [ ] AOF rewrite and tombstones
- [ ] ordered change stream
- [ ] snapshot and offset bootstrap
- [ ] leader/follower async replication
- [ ] resume from persisted offset
- [ ] read-your-writes and monotonic reads
- [ ] WATCH/CAS-style optimistic writes
- [ ] event-time windows and late corrections
