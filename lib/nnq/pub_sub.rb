# frozen_string_literal: true

require_relative "socket"
require_relative "routing/pub"
require_relative "routing/sub"

module NNQ
  # PUB side of the pub/sub pattern (nng pub0). Broadcasts every
  # message to every connected SUB. Per-peer bounded send queues —
  # a slow peer drops messages instead of blocking fast peers.
  # Defaults to listening.
  #
  class PUB < Socket
    def send(body)
      Reactor.run { @engine.routing.send(body) }
    end


    private

    def protocol
      Protocol::SP::Protocols::PUB_V0
    end


    def build_routing(engine)
      Routing::Pub.new(engine)
    end
  end


  # SUB side of the pub/sub pattern (nng sub0). Applies local
  # byte-prefix filtering. Empty subscription set means no messages
  # are delivered — matching nng (unlike pre-4.x ZeroMQ). Defaults
  # to dialing.
  #
  class SUB < Socket
    # Subscribes to +prefix+. Bytes-level match. The empty string
    # matches everything.
    #
    # @param prefix [String]
    def subscribe(prefix)
      Reactor.run { @engine.routing.subscribe(prefix) }
    end


    # Removes a previously-added subscription. No-op if not present.
    #
    # @param prefix [String]
    def unsubscribe(prefix)
      Reactor.run { @engine.routing.unsubscribe(prefix) }
    end


    # @return [String, nil]
    def receive
      Reactor.run { @engine.routing.receive }
    end


    private

    def protocol
      Protocol::SP::Protocols::SUB_V0
    end


    def build_routing(_engine)
      Routing::Sub.new
    end
  end
end
