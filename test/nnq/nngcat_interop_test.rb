# frozen_string_literal: true

require_relative "../test_helper"
require "tempfile"

module NNQ
  class NngcatInteropTest < Minitest::Test
    def setup
      skip "nngcat not installed" unless system("which nngcat >/dev/null 2>&1")
    end


    # nngcat --pull0 listens, NNQ::PUSH dials.
    def test_nnq_push_to_nngcat_pull
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
    def test_nngcat_push_to_nnq_pull
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
  end
end
