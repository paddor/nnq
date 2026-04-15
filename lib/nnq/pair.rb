# frozen_string_literal: true

require_relative "socket"
require_relative "routing/pair"

module NNQ
  # PAIR (nng pair0): exclusive bidirectional channel with a single
  # peer. First peer to connect wins; subsequent peers are dropped
  # until the current one disconnects. No SP header on the wire.
  #
  class PAIR0 < Socket
    def send(body)
      body = frozen_binary(body)
      Reactor.run { @engine.routing.send(body) }
    end


    def receive
      Reactor.run { @engine.routing.receive }
    end


    private


    def protocol
      Protocol::SP::Protocols::PAIR_V0
    end


    def build_routing(engine)
      Routing::Pair.new(engine)
    end
  end


  PAIR = PAIR0
end
