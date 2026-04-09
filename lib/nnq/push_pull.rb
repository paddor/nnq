# frozen_string_literal: true

require_relative "socket"
require_relative "routing/push"
require_relative "routing/pull"

module NNQ
  # PUSH side of the pipeline pattern (nng push0). Round-robins messages
  # across all live PULL peers. Defaults to dialing.
  #
  class PUSH < Socket
    def send(body)
      Reactor.run { @engine.send_message(body) }
    end


    private

    def protocol
      Protocol::SP::Protocols::PUSH_V0
    end


    def build_routing(engine)
      Routing::Push.new(engine.connections, engine.new_pipe)
    end
  end


  # PULL side of the pipeline pattern (nng pull0). Fair-queues messages
  # from all live PUSH peers into one unbounded receive queue. Defaults
  # to listening.
  #
  class PULL < Socket
    def receive
      Reactor.run { @engine.receive_message }
    end


    private

    def protocol
      Protocol::SP::Protocols::PULL_V0
    end


    def build_routing(_engine)
      Routing::Pull.new
    end
  end
end
