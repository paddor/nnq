# frozen_string_literal: true

require_relative "lib/nnq/version"

Gem::Specification.new do |s|
  s.name     = "nnq"
  s.version  = NNQ::VERSION
  s.authors  = ["Patrik Wenger"]
  s.email    = ["paddor@gmail.com"]
  s.summary  = "Pure Ruby NNG (Scalability Protocols) on Async"
  s.description = "Pure Ruby implementation of nanomsg's Scalability Protocols " \
                  "(SP) on top of async + io-stream. Per-socket HWM, " \
                  "opportunistic batching, wire-compatible with libnng over " \
                  "inproc/ipc/tcp."
  s.homepage = "https://github.com/paddor/nnq"
  s.license  = "ISC"

  s.required_ruby_version = ">= 4.0"

  s.files = Dir["lib/**/*.rb", "README.md", "LICENSE", "CHANGELOG.md"]

  s.add_dependency "async", "~> 2.38"
  s.add_dependency "io-stream", "~> 0.11"
  s.add_dependency "protocol-sp", ">= 0.3"
end
