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
