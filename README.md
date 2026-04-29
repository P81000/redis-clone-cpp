
# Redis Clone (C++)

High-performance in-memory key-value store inspired by Redis, built in C++.

This project implements a lightweight Redis-like server capable of handling basic commands such as `PING`, `SET`, and `GET`, using low-level socket programming and an event-driven architecture.

It focuses on core systems programming concepts such as:
- Event loops
- TCP socket communication
- Command parsing
- In-memory data storage
- Low-latency request handling

Built as part of a systems programming challenge, but extended with a focus on performance and architectural understanding.

---

## Features

- Custom TCP server in C++
- RESP-like protocol parsing
- In-memory key-value store
- Basic command execution (`PING`, `SET`, `GET`)
- Event-driven architecture

---

## Design Focus

This implementation emphasizes:
- Low-level systems design
- Minimal latency request handling
- Efficient memory usage
- Scalable architecture patterns

---

## Build & Run

```bash
cmake .
make
./your_program.sh
