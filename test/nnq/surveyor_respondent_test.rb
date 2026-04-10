# frozen_string_literal: true

require_relative "../test_helper"

describe NNQ::SURVEYOR0 do
  it "collects a reply from a single respondent" do
    Sync do
      surveyor   = NNQ::SURVEYOR0.bind("tcp://127.0.0.1:0")
      respondent = NNQ::RESPONDENT0.connect(surveyor.last_endpoint)

      surveyor.peer_connected.wait

      surveyor.send_survey("ping")
      body = respondent.receive
      assert_equal "ping", body
      respondent.send_reply("pong")

      reply = surveyor.receive
      assert_equal "pong", reply
    ensure
      respondent&.close
      surveyor&.close
    end
  end


  it "collects replies from multiple respondents" do
    Sync do
      surveyor = NNQ::SURVEYOR0.bind("tcp://127.0.0.1:0")
      r1       = NNQ::RESPONDENT0.connect(surveyor.last_endpoint)
      r2       = NNQ::RESPONDENT0.connect(surveyor.last_endpoint)

      50.times do
        break if surveyor.connection_count >= 2
        sleep 0.01
      end

      surveyor.send_survey("roll call")

      body1 = r1.receive
      body2 = r2.receive
      assert_equal "roll call", body1
      assert_equal "roll call", body2

      r1.send_reply("here-1")
      r2.send_reply("here-2")

      replies = [surveyor.receive, surveyor.receive].sort
      assert_equal ["here-1", "here-2"], replies
    ensure
      r1&.close
      r2&.close
      surveyor&.close
    end
  end


  it "times out when no replies arrive" do
    Sync do
      surveyor = NNQ::SURVEYOR0.bind("tcp://127.0.0.1:0")
      surveyor.options.survey_time = 0.05

      respondent = NNQ::RESPONDENT0.connect(surveyor.last_endpoint)
      surveyor.peer_connected.wait

      surveyor.send_survey("hello")
      # Respondent receives but does not reply.
      respondent.receive

      assert_raises(NNQ::TimedOut) { surveyor.receive }
    ensure
      respondent&.close
      surveyor&.close
    end
  end


  it "drops stale replies from a previous survey" do
    Sync do |task|
      surveyor   = NNQ::SURVEYOR0.bind("tcp://127.0.0.1:0")
      respondent = NNQ::RESPONDENT0.connect(surveyor.last_endpoint)
      surveyor.options.survey_time = 0.5

      surveyor.peer_connected.wait

      # First survey — respondent receives but we abandon before reply.
      surveyor.send_survey("first")
      body = respondent.receive
      assert_equal "first", body

      # Reply to first survey — carries the first survey's ID.
      respondent.send_reply("late-reply")

      # Start second survey — new ID. The late reply above will be
      # silently dropped because its survey ID doesn't match.
      surveyor.send_survey("second")
      body2 = respondent.receive
      assert_equal "second", body2
      respondent.send_reply("reply-to-second")

      # Surveyor should only see the second reply.
      reply = surveyor.receive
      assert_equal "reply-to-second", reply
    ensure
      respondent&.close
      surveyor&.close
    end
  end


  it "handles multiple request-reply rounds" do
    n = 50
    Sync do
      surveyor   = NNQ::SURVEYOR0.bind("tcp://127.0.0.1:0")
      respondent = NNQ::RESPONDENT0.connect(surveyor.last_endpoint)

      surveyor.peer_connected.wait

      n.times do |i|
        surveyor.send_survey("q#{i}")
        body = respondent.receive
        assert_equal "q#{i}", body
        respondent.send_reply("a#{i}")
        reply = surveyor.receive
        assert_equal "a#{i}", reply
      end
    ensure
      respondent&.close
      surveyor&.close
    end
  end
end
