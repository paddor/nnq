# frozen_string_literal: true

require_relative "../test_helper"

module NNQ
  class ReqRepTest < Minitest::Test
    def test_round_trip_over_tcp
      Sync do |task|
        rep = NNQ::REP.bind("tcp://127.0.0.1:0")
        req = NNQ::REQ.connect(rep.last_endpoint)

        server = task.async do
          body = rep.receive
          rep.send_reply("echo: #{body}")
        end

        reply = req.send_request("hello")
        assert_equal "echo: hello", reply
        server.wait
      ensure
        req&.close
        rep&.close
      end
    end


    def test_many_requests
      n = 100
      Sync do |task|
        rep = NNQ::REP.bind("tcp://127.0.0.1:0")
        req = NNQ::REQ.connect(rep.last_endpoint)

        server = task.async do
          n.times do
            body = rep.receive
            rep.send_reply(body.reverse)
          end
        end

        n.times do |i|
          assert_equal "req#{i}".reverse, req.send_request("req#{i}")
        end
        server.wait
      ensure
        req&.close
        rep&.close
      end
    end


    def test_concurrent_request_raises
      Sync do |task|
        rep = NNQ::REP.bind("tcp://127.0.0.1:0")
        req = NNQ::REQ.connect(rep.last_endpoint)

        # Start a request that will block (the REP never replies).
        slow = task.async { req.send_request("blocked") }
        # Give the first call time to register as outstanding.
        sleep(0.01)

        assert_raises(NNQ::Error) { req.send_request("second") }

        slow.stop
      ensure
        req&.close
        rep&.close
      end
    end
  end
end
