# frozen_string_literal: true

require_relative "../test_helper"
require "tempfile"
require "tmpdir"

describe "nngcat interop" do
  before do
    skip "nngcat not installed" unless system("which nngcat >/dev/null 2>&1")
  end


  # nngcat --pull0 listens, NNQ::PUSH dials.
  it "NNQ::PUSH → nngcat --pull0 over tcp" do
    out = Tempfile.new("nng-out")
    out.close
    port = 5571

    nng_pid = spawn("nngcat", "--pull0", "--listen", "tcp://127.0.0.1:#{port}",
                    "--count", "3", "--quoted",
                    out: out.path, err: File::NULL)

    Sync do
      # Wait until nngcat is listening.
      push = nil
      20.times do
        push = NNQ::PUSH.connect("tcp://127.0.0.1:#{port}") rescue (sleep(0.05); nil)
        break if push
      end
      flunk "could not connect to nngcat" unless push

      push.send("alpha")
      push.send("beta")
      push.send("gamma")
      push.close
    end

    Process.wait(nng_pid)
    contents = File.read(out.path)
    assert_match(/alpha/, contents)
    assert_match(/beta/, contents)
    assert_match(/gamma/, contents)
  ensure
    out&.unlink
    Process.kill("KILL", nng_pid) rescue nil
  end


  # NNQ::PULL listens, nngcat --push0 dials.
  it "nngcat --push0 → NNQ::PULL over tcp" do
    port    = 5572
    nng_pid = nil
    Sync do
      pull = NNQ::PULL.bind("tcp://127.0.0.1:#{port}")
      nng_pid = spawn("nngcat", "--push0", "--dial", "tcp://127.0.0.1:#{port}",
                      "--data", "hello-from-nngcat",
                      "--count", "1",
                      out: File::NULL, err: File::NULL)
      body = pull.receive
      assert_equal "hello-from-nngcat", body
    ensure
      pull&.close # must close *inside* Sync — accept/recv loops keep the reactor alive
    end
    Process.wait(nng_pid)
  ensure
    Process.kill("KILL", nng_pid) rescue nil
  end


  # NNQ::PUSH dials over IPC, nngcat --pull0 listens.
  it "NNQ::PUSH → nngcat --pull0 over ipc" do
    Dir.mktmpdir do |dir|
      path     = "#{dir}/nnq.sock"
      endpoint = "ipc://#{path}"
      out      = Tempfile.new("nng-ipc")
      out.close

      nng_pid = spawn("nngcat", "--pull0", "--listen", endpoint,
                      "--count", "3", "--quoted",
                      out: out.path, err: File::NULL)

      Sync do
        push = nil
        20.times do
          push = NNQ::PUSH.connect(endpoint) rescue (sleep(0.05); nil)
          break if push
        end
        flunk "could not connect to nngcat over ipc" unless push
        push.send("alpha")
        push.send("beta")
        push.send("gamma")
        push.close
      end

      Process.wait(nng_pid)
      contents = File.read(out.path)
      assert_match(/alpha/, contents)
      assert_match(/beta/,  contents)
      assert_match(/gamma/, contents)
    ensure
      out&.unlink
      Process.kill("KILL", nng_pid) rescue nil
    end
  end


  # NNQ::PULL listens over IPC, nngcat --push0 dials.
  it "nngcat --push0 → NNQ::PULL over ipc" do
    Dir.mktmpdir do |dir|
      endpoint = "ipc://#{dir}/nnq.sock"
      nng_pid  = nil
      Sync do
        pull = NNQ::PULL.bind(endpoint)
        nng_pid = spawn("nngcat", "--push0", "--dial", endpoint,
                        "--data", "hello-over-ipc", "--count", "1",
                        out: File::NULL, err: File::NULL)
        assert_equal "hello-over-ipc", pull.receive
      ensure
        pull&.close
      end
      Process.wait(nng_pid)
    ensure
      Process.kill("KILL", nng_pid) rescue nil
    end
  end


  # NNQ::PAIR dials, nngcat --pair0 listens.
  it "NNQ::PAIR → nngcat --pair0" do
    out = Tempfile.new("nng-pair")
    out.close
    port = 5573

    nng_pid = spawn("nngcat", "--pair0", "--listen", "tcp://127.0.0.1:#{port}",
                    "--count", "1", "--quoted",
                    out: out.path, err: File::NULL)

    Sync do
      pair = nil
      20.times do
        pair = NNQ::PAIR.connect("tcp://127.0.0.1:#{port}") rescue (sleep(0.05); nil)
        break if pair
      end
      flunk "could not connect to nngcat" unless pair
      pair.send("hello-pair")
      pair.close
    end

    Process.wait(nng_pid)
    assert_match(/hello-pair/, File.read(out.path))
  ensure
    out&.unlink
    Process.kill("KILL", nng_pid) rescue nil
  end


  # NNQ::REQ dials, nngcat --rep0 listens and echoes.
  it "NNQ::REQ → nngcat --rep0" do
    port = 5574
    nng_pid = spawn("nngcat", "--rep0", "--listen", "tcp://127.0.0.1:#{port}",
                    "--data", "pong",
                    out: File::NULL, err: File::NULL)

    Sync do
      req = nil
      20.times do
        req = NNQ::REQ.connect("tcp://127.0.0.1:#{port}") rescue (sleep(0.05); nil)
        break if req
      end
      flunk "could not connect to nngcat" unless req
      reply = req.send_request("ping")
      assert_equal "pong", reply
      req.close
    end

    Process.kill("KILL", nng_pid) rescue nil
    Process.wait(nng_pid) rescue nil
  ensure
    Process.kill("KILL", nng_pid) rescue nil
  end


  # NNQ::REP listens, nngcat --req0 dials and sends a request.
  it "nngcat --req0 → NNQ::REP" do
    port = 5575
    out  = Tempfile.new("nng-req")
    out.close
    nng_pid = nil
    Sync do
      rep = NNQ::REP.bind("tcp://127.0.0.1:#{port}")
      nng_pid = spawn("nngcat", "--req0", "--dial", "tcp://127.0.0.1:#{port}",
                      "--data", "ping", "--quoted",
                      out: out.path, err: File::NULL)
      body = rep.receive
      assert_equal "ping", body
      rep.send_reply("pong")
    ensure
      rep&.close
    end
    Process.wait(nng_pid)
    assert_match(/pong/, File.read(out.path))
  ensure
    out&.unlink
    Process.kill("KILL", nng_pid) rescue nil
  end
end
