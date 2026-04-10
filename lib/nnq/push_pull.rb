# frozen_string_literal: true

require_relative "socket"
require_relative "routing/push"
require_relative "routing/pull"

module NNQ
  # PUSH side of the pipeline pattern (nng push0). Enqueues onto a single
  # bounded send queue (`send_hwm`); per-peer send pumps work-steal from
  # it. Defaults to dialing.
  #
  class PUSH0 < Socket
    def send(body)
      Reactor.run { @engine.routing.send(body) }
    end


    private

    def protocol
      Protocol::SP::Protocols::PUSH_V0
    end


    def build_routing(engine)
      Routing::Push.new(engine)
    end
  end


  # PULL side of the pipeline pattern (nng pull0). Fair-queues messages
  # from all live PUSH peers into one unbounded receive queue. Defaults
  # to listening.
  #
  class PULL0 < Socket
    def receive
      Reactor.run do
        if (timeout = @engine.options.read_timeout)
          Fiber.scheduler.with_timeout(timeout) { @engine.routing.receive }
        else
          @engine.routing.receive
        end
      end
    end


    private

    def protocol
      Protocol::SP::Protocols::PULL_V0
    end


    def build_routing(_engine)
      Routing::Pull.new
    end
  end

  PUSH = PUSH0
  PULL = PULL0
end
