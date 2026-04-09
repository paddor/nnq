# frozen_string_literal: true

require_relative "../test_helper"

describe NNQ::PUB do
  it "delivers to a subscribed peer" do
    Sync do
      pub = NNQ::PUB.bind("tcp://127.0.0.1:0")
      sub = NNQ::SUB.connect(pub.last_endpoint)
      sub.subscribe("")
      50.times do
        break if pub.connection_count >= 1
        sleep 0.01
      end
      pub.send("hello")
      Async::Task.current.with_timeout(2) do
        assert_equal "hello", sub.receive
      end
    ensure
      sub&.close
      pub&.close
    end
  end


  it "delivers to a second fresh pair" do
    Sync do
      pub = NNQ::PUB.bind("tcp://127.0.0.1:0")
      sub = NNQ::SUB.connect(pub.last_endpoint)
      sub.subscribe("")
      50.times do
        break if pub.connection_count >= 1
        sleep 0.01
      end
      pub.send("hello")
      Async::Task.current.with_timeout(2) do
        assert_equal "hello", sub.receive
      end
    ensure
      sub&.close
      pub&.close
    end
  end
end
