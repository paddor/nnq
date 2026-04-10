# frozen_string_literal: true

require_relative "socket"
require_relative "routing/bus"

module NNQ
  # BUS (nng bus0): best-effort bidirectional mesh. Every message sent
  # goes to all directly connected peers. Every message received from
  # any peer is delivered to the application. Self-pairing (BUS ↔ BUS).
  #
  # Send never blocks — if a peer's queue is full, the message is
  # dropped for that peer (matching nng's best-effort semantics).
  #
  class BUS0 < Socket
    def send(body)
      Reactor.run { @engine.routing.send(body) }
    end


    def receive
      Reactor.run { @engine.routing.receive }
    end


    private

    def protocol
      Protocol::SP::Protocols::BUS_V0
    end


    def build_routing(engine)
      Routing::Bus.new(engine)
    end
  end
end
