# frozen_string_literal: true

# Run all pattern benchmarks sequentially, appending results to bench/results.jsonl.

ENV["NNQ_BENCH_RUN_ID"] = Time.now.strftime("%Y-%m-%dT%H:%M:%S")

%w[push_pull req_rep pair pub_sub].each do |pattern|
  system("ruby", "--yjit", File.join(__dir__, pattern, "nnq.rb")) || abort("#{pattern} failed")
end
