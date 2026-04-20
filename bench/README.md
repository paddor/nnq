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
| 128 B | 1.08M msg/s / 138 MB/s | 429.7k msg/s / 55.0 MB/s | 373.3k msg/s / 47.8 MB/s |
| 512 B | 1.10M msg/s / 564 MB/s | 314.3k msg/s / 161 MB/s | 286.3k msg/s / 147 MB/s |
| 2 KiB | 1.12M msg/s / 2.30 GB/s | 235.7k msg/s / 483 MB/s | 218.8k msg/s / 448 MB/s |
| 8 KiB | 1.10M msg/s / 9.04 GB/s | 110.7k msg/s / 907 MB/s | 101.1k msg/s / 829 MB/s |
| 32 KiB | 796.4k msg/s / 26.10 GB/s | 38.4k msg/s / 1.26 GB/s | 34.4k msg/s / 1.13 GB/s |

### 3 peers

| Message size | inproc | ipc | tcp |
|---|---|---|---|
| 128 B | 1.32M msg/s / 169 MB/s | 436.2k msg/s / 55.8 MB/s | 385.8k msg/s / 49.4 MB/s |
| 512 B | 1.22M msg/s / 624 MB/s | 350.6k msg/s / 180 MB/s | 255.1k msg/s / 131 MB/s |
| 2 KiB | 1.23M msg/s / 2.51 GB/s | 228.6k msg/s / 468 MB/s | 209.5k msg/s / 429 MB/s |
| 8 KiB | 1.10M msg/s / 9.01 GB/s | 106.2k msg/s / 870 MB/s | 100.9k msg/s / 827 MB/s |
| 32 KiB | 882.4k msg/s / 28.92 GB/s | 35.4k msg/s / 1.16 GB/s | 32.7k msg/s / 1.07 GB/s |

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
| 128 B | 11.1 µs | 39.9 µs | 56.7 µs |
| 512 B | 11.9 µs | 41.2 µs | 57.4 µs |
| 2 KiB | 12.1 µs | 49.0 µs | 64.4 µs |
| 8 KiB | 13.7 µs | 56.7 µs | 67.4 µs |
| 32 KiB | 23.9 µs | 82.7 µs | 83.2 µs |

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
