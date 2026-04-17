# Benchmarks

Each cell is the fastest of 3 timed rounds (~1 s each) after a calibration
warmup, so transient scheduler/GC jitter is filtered out. Between-run variance
on the same machine is ~5-15 % depending on transport; treat single-digit
deltas across runs as noise.

Regenerate the tables below from the latest run in `results.jsonl`:

```sh
ruby bench/report.rb --update-readme
```

## Throughput (PUSH/PULL, msg/s)

```
┌──────┐       ┌──────┐
│ PUSH │──────→│ PULL │
└──────┘       └──────┘
```

<!-- BEGIN push_pull -->
### 1 peer

| Message size | inproc | ipc | tcp |
|---|---|---|---|
| 128 B | 518.3k msg/s / 66.3 MB/s | 499.2k msg/s / 63.9 MB/s | 548.5k msg/s / 70.2 MB/s |
| 512 B | 482.9k msg/s / 247 MB/s | 433.2k msg/s / 222 MB/s | 467.3k msg/s / 239 MB/s |
| 2 KiB | 313.6k msg/s / 642 MB/s | 259.9k msg/s / 532 MB/s | 277.2k msg/s / 568 MB/s |
| 8 KiB | 130.9k msg/s / 1.07 GB/s | 111.8k msg/s / 916 MB/s | 125.7k msg/s / 1.03 GB/s |
| 32 KiB | 39.4k msg/s / 1.29 GB/s | 35.5k msg/s / 1.16 GB/s | 37.2k msg/s / 1.22 GB/s |

### 3 peers

| Message size | inproc | ipc | tcp |
|---|---|---|---|
| 128 B | 574.5k msg/s / 73.5 MB/s | 456.5k msg/s / 58.4 MB/s | 591.9k msg/s / 75.8 MB/s |
| 512 B | 528.5k msg/s / 271 MB/s | 451.6k msg/s / 231 MB/s | 527.6k msg/s / 270 MB/s |
| 2 KiB | 305.1k msg/s / 625 MB/s | 309.7k msg/s / 634 MB/s | 307.9k msg/s / 631 MB/s |
| 8 KiB | 137.9k msg/s / 1.13 GB/s | 134.3k msg/s / 1.10 GB/s | 131.6k msg/s / 1.08 GB/s |
| 32 KiB | 38.9k msg/s / 1.27 GB/s | 38.4k msg/s / 1.26 GB/s | 37.2k msg/s / 1.22 GB/s |

<!-- END push_pull -->

## Round-trip latency (REQ/REP, µs)

```
┌─────┐  req   ┌─────┐
│ REQ │───────→│ REP │
│     │←───────│     │
└─────┘  rep   └─────┘
```

Round-trip = one `req.send_request` (which sends + blocks for the reply).
Latency is `1 / msgs_s` converted to µs.

<!-- BEGIN req_rep -->
| Message size | inproc | ipc | tcp |
|---|---|---|---|
| 128 B | 35.1 µs | 36.3 µs | 47.7 µs |
| 512 B | 36.4 µs | 37.7 µs | 48.2 µs |
| 2 KiB | 39.2 µs | 39.3 µs | 50.1 µs |
| 8 KiB | 44.7 µs | 47.9 µs | 58.6 µs |
| 32 KiB | 61.7 µs | 62.2 µs | 72.7 µs |

<!-- END req_rep -->

## io_uring

With `liburing-dev` installed, io-event uses io_uring instead of epoll.
Inproc throughput jumps significantly. IPC and TCP are within variance.

```sh
# Debian/Ubuntu
sudo apt install liburing-dev
gem pristine io-event
```

## Running

```sh
# Full suite (one run_id shared across patterns for cross-pattern comparison)
bundle exec ruby --yjit bench/run_all.rb

# Or per-pattern with an explicit run_id:
RUN_ID=$(date +%Y-%m-%dT%H:%M:%S)
for d in push_pull req_rep pair pub_sub; do
  NNQ_BENCH_RUN_ID=$RUN_ID bundle exec ruby --yjit bench/$d/nnq.rb
done

# Regression report (latest vs previous run)
bundle exec ruby bench/report.rb

# Regenerate README tables from the latest run
bundle exec ruby bench/report.rb --update-readme

# Full comparison table
bundle exec ruby bench/report.rb --all
```
