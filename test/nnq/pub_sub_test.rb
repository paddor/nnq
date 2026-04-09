$LOAD_PATH.unshift("lib", "test")
require "test_helper"

describe NNQ::PUB do
  it "works" do
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


  it "works again" do
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
