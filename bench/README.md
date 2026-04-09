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
| 64 B | 529.4k msg/s / 33.9 MB/s | 481.6k msg/s / 30.8 MB/s | 558.4k msg/s / 35.7 MB/s |
| 1 KiB | 357.7k msg/s / 366 MB/s | 344.0k msg/s / 352 MB/s | 349.1k msg/s / 357 MB/s |
| 8 KiB | 124.7k msg/s / 1.02 GB/s | 122.9k msg/s / 1.01 GB/s | 120.4k msg/s / 986 MB/s |
| 64 KiB | 21.9k msg/s / 1.43 GB/s | 20.0k msg/s / 1.31 GB/s | 17.6k msg/s / 1.15 GB/s |

### 3 peers

| Message size | inproc | ipc | tcp |
|---|---|---|---|
| 64 B | 570.5k msg/s / 36.5 MB/s | 487.0k msg/s / 31.2 MB/s | 556.5k msg/s / 35.6 MB/s |
| 1 KiB | 323.1k msg/s / 331 MB/s | 317.0k msg/s / 325 MB/s | 317.3k msg/s / 325 MB/s |
| 8 KiB | 125.0k msg/s / 1.02 GB/s | 124.0k msg/s / 1.02 GB/s | 119.3k msg/s / 978 MB/s |
| 64 KiB | 21.2k msg/s / 1.39 GB/s | 20.1k msg/s / 1.32 GB/s | 18.5k msg/s / 1.21 GB/s |

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
| 64 B | 36.2 µs | 35.2 µs | 52.0 µs |
| 1 KiB | 37.5 µs | 38.5 µs | 54.5 µs |
| 8 KiB | 49.3 µs | 49.0 µs | 65.3 µs |
| 64 KiB | 119 µs | 123 µs | 156 µs |

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
