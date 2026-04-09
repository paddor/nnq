# frozen_string_literal: true

require_relative "../../test_helper"
require "tmpdir"

module NNQ
  class IPCTransportTest < Minitest::Test
    def test_push_pull_over_ipc
      Dir.mktmpdir do |dir|
        endpoint = "ipc://#{dir}/nnq.sock"
        Sync do
          pull = NNQ::PULL.bind(endpoint)
          push = NNQ::PUSH.connect(endpoint)
          push.send("alpha")
          push.send("beta")
          assert_equal "alpha", pull.receive
          assert_equal "beta",  pull.receive
        ensure
          push&.close
          pull&.close
        end
      end
    end


    def test_push_pull_over_abstract_ipc
      skip "abstract namespace is Linux-only" unless RUBY_PLATFORM.include?("linux")
      endpoint = "ipc://@nnq-test-#{Process.pid}-#{rand(1 << 30)}"
      Sync do
        pull = NNQ::PULL.bind(endpoint)
        push = NNQ::PUSH.connect(endpoint)
        push.send("hello")
        assert_equal "hello", pull.receive
      ensure
        push&.close
        pull&.close
      end
    end


    def test_pair_round_trip_over_ipc
      Dir.mktmpdir do |dir|
        endpoint = "ipc://#{dir}/pair.sock"
        Sync do
          a = NNQ::PAIR.bind(endpoint)
          b = NNQ::PAIR.connect(endpoint)
          a.send("ping")
          assert_equal "ping", b.receive
          b.send("pong")
          assert_equal "pong", a.receive
        ensure
          a&.close
          b&.close
        end
      end
    end


    def test_req_rep_round_trip_over_ipc
      Dir.mktmpdir do |dir|
        endpoint = "ipc://#{dir}/rr.sock"
        Sync do
          rep = NNQ::REP.bind(endpoint)
          req = NNQ::REQ.connect(endpoint)
          task = Async do
            body = rep.receive
            rep.send_reply(body.upcase)
          end
          assert_equal "HELLO", req.send_request("hello")
          task.wait
        ensure
          req&.close
          rep&.close
        end
      end
    end


    def test_stale_socket_file_is_removed
      Dir.mktmpdir do |dir|
        endpoint = "ipc://#{dir}/nnq.sock"
        path     = "#{dir}/nnq.sock"
        File.write(path, "") # stale file from a prior crash
        Sync do
          pull = NNQ::PULL.bind(endpoint)
          push = NNQ::PUSH.connect(endpoint)
          push.send("ok")
          assert_equal "ok", pull.receive
        ensure
          push&.close
          pull&.close
        end
      end
    end
  end
end
