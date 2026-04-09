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


    def test_new_send_request_cancels_outstanding
      Sync do |task|
        rep = NNQ::REP.bind("tcp://127.0.0.1:0")
        req = NNQ::REQ.connect(rep.last_endpoint)

        # Server answers only the SECOND request — the first is abandoned.
        server = task.async do
          rep.receive  # first body is abandoned — never replied to
          body = rep.receive
          rep.send_reply("answered: #{body}")
        end

        blocked = task.async do
          assert_raises(NNQ::RequestCancelled) { req.send_request("first") }
        end
        sleep(0.01) # let the first request register as outstanding

        reply = req.send_request("second")
        assert_equal "answered: second", reply
        blocked.wait
        server.wait
      ensure
        req&.close
        rep&.close
      end
    end


    def test_rep_discards_pending_on_second_receive
      Sync do |task|
        rep = NNQ::REP.bind("tcp://127.0.0.1:0")
        req1 = NNQ::REQ.connect(rep.last_endpoint)
        req2 = NNQ::REQ.connect(rep.last_endpoint)

        # Two separate REQ sockets send in order.
        first = task.async { req1.send_request("drop-me") }
        sleep(0.01)
        second = task.async { req2.send_request("keep-me") }
        sleep(0.01)

        # REP reads the first, then calls receive again — first is dropped.
        assert_equal "drop-me", rep.receive
        body2 = rep.receive
        assert_equal "keep-me", body2
        rep.send_reply("ok")

        assert_equal "ok", second.wait
        # first is still outstanding (never replied) — cancel by closing.
        first.stop
      ensure
        req1&.close
        req2&.close
        rep&.close
      end
    end


    def test_send_reply_without_pending_raises
      Sync do
        rep = NNQ::REP.bind("tcp://127.0.0.1:0")
        assert_raises(NNQ::Error) { rep.send_reply("nope") }
      ensure
        rep&.close
      end
    end
  end
end
