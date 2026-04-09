# frozen_string_literal: true

module NNQ
  # Per-socket configuration. Deliberately tiny — no HWM, no conflate, no
  # heartbeat command (nng uses TCP keepalive instead). New options should
  # match nng socket option names where they exist (`recv_max_size`,
  # `reconnect_time`, `send_buf`, etc.).
  #
  class Options
    attr_accessor :linger
    attr_accessor :read_timeout
    attr_accessor :write_timeout
    attr_accessor :reconnect_interval
    attr_accessor :max_message_size

    def initialize(linger: 0)
      @linger             = linger
      @read_timeout       = nil
      @write_timeout      = nil
      @reconnect_interval = 0.1
      @max_message_size   = nil
    end
  end
end
