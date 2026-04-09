# frozen_string_literal: true

module NNQ
  # Per-socket configuration. Deliberately tiny — no conflate, no heartbeat
  # command (nng uses TCP keepalive instead). New options should match nng
  # socket option names where they exist (`recv_max_size`, `reconnect_time`,
  # `send_buf`, etc.).
  #
  # `send_hwm` bounds *one shared queue per socket*, not per peer. See
  # DESIGN.md "Per-socket HWM".
  #
  class Options
    DEFAULT_HWM = 1000

    attr_accessor :linger
    attr_accessor :read_timeout
    attr_accessor :write_timeout
    attr_accessor :reconnect_interval
    attr_accessor :max_message_size
    attr_accessor :send_hwm

    def initialize(linger: nil, send_hwm: DEFAULT_HWM)
      @linger             = linger
      @read_timeout       = nil
      @write_timeout      = nil
      @reconnect_interval = 0.1
      @max_message_size   = nil
      @send_hwm           = send_hwm
    end
  end
end
