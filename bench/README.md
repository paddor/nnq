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
| 128 B | 463.3k msg/s / 59.3 MB/s | 438.7k msg/s / 56.2 MB/s | 507.9k msg/s / 65.0 MB/s |
| 512 B | 396.6k msg/s / 203 MB/s | 391.7k msg/s / 201 MB/s | 431.0k msg/s / 221 MB/s |
| 2 KiB | 245.4k msg/s / 503 MB/s | 230.3k msg/s / 472 MB/s | 240.2k msg/s / 492 MB/s |
| 8 KiB | 112.5k msg/s / 921 MB/s | 107.0k msg/s / 877 MB/s | 115.1k msg/s / 943 MB/s |
| 32 KiB | 38.3k msg/s / 1.26 GB/s | 34.4k msg/s / 1.13 GB/s | 35.4k msg/s / 1.16 GB/s |

### 3 peers

| Message size | inproc | ipc | tcp |
|---|---|---|---|
| 128 B | 477.6k msg/s / 61.1 MB/s | 439.5k msg/s / 56.3 MB/s | 502.3k msg/s / 64.3 MB/s |
| 512 B | 417.8k msg/s / 214 MB/s | 374.5k msg/s / 192 MB/s | 392.3k msg/s / 201 MB/s |
| 2 KiB | 248.5k msg/s / 509 MB/s | 237.6k msg/s / 487 MB/s | 240.4k msg/s / 492 MB/s |
| 8 KiB | 117.2k msg/s / 960 MB/s | 112.0k msg/s / 918 MB/s | 111.9k msg/s / 916 MB/s |
| 32 KiB | 38.4k msg/s / 1.26 GB/s | 36.6k msg/s / 1.20 GB/s | 33.6k msg/s / 1.10 GB/s |

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
| 128 B | 45.5 µs | 40.7 µs | 55.7 µs |
| 512 B | 47.5 µs | 41.5 µs | 55.7 µs |
| 2 KiB | 55.8 µs | 46.3 µs | 57.1 µs |
| 8 KiB | 55.0 µs | 58.1 µs | 69.5 µs |
| 32 KiB | 92.9 µs | 92.3 µs | 108 µs |

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
