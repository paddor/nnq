# frozen_string_literal: true

require "socket"
require "io/stream"

module NNQ
  module Transport
    # In-process transport. Both peers live in the same process and
    # exchange frames over a Unix socketpair — no network, no address.
    #
    # Unlike omq's DirectPipe, inproc here still runs through
    # Protocol::SP: the socketpair just replaces TCP. Kernel buffering
    # across the pair is plenty to avoid contention for typical
    # in-process message sizes, and reusing the SP handshake + framing
    # keeps the transport ~40 LOC instead of a parallel Connection
    # implementation.
    #
    module Inproc
      @registry = {}
      @mutex    = Mutex.new


      class << self
        # Binds +engine+ to +endpoint+ in the process-global registry.
        #
        # @param endpoint [String] e.g. "inproc://my-endpoint"
        # @param engine [Engine]
        # @return [Listener]
        def bind(endpoint, engine)
          @mutex.synchronize do
            raise Error, "inproc endpoint already bound: #{endpoint}" if @registry.key?(endpoint)
            @registry[endpoint] = engine
          end

          Listener.new(endpoint)
        end


        # Connects +engine+ to a bound inproc endpoint. Creates a Unix
        # socketpair, hands one side to the bound engine (accepted),
        # the other to the connecting engine (connected). Both sides
        # run the normal SP handshake concurrently.
        #
        # @param endpoint [String]
        # @param engine [Engine]
        # @return [void]
        def connect(endpoint, engine)
          bound = @mutex.synchronize { @registry[endpoint] }
          raise Error, "inproc endpoint not bound: #{endpoint}" unless bound

          a, b = UNIXSocket.pair

          # Handshake on the bound side must run concurrently with
          # ours — if we called bound.handle_accepted synchronously
          # it would block on reading our greeting before we've had
          # a chance to write it.
          bound.spawn_task(annotation: "nnq inproc accept #{endpoint}") do
            bound.handle_accepted(IO::Stream::Buffered.wrap(b), endpoint: endpoint)
          end
          engine.handle_connected(IO::Stream::Buffered.wrap(a), endpoint: endpoint)
        end


        # Removes +endpoint+ from the registry. Called by Listener#stop.
        def unbind(endpoint)
          @mutex.synchronize { @registry.delete(endpoint) }
        end


        # Clears the registry. For tests.
        def reset!
          @mutex.synchronize { @registry.clear }
        end
      end


      # A bound inproc endpoint. Owns no fibers — just a registry entry.
      class Listener
        attr_reader :endpoint


        def initialize(endpoint)
          @endpoint = endpoint
        end


        # No accept loop: inproc connects synchronously.
        def start_accept_loop(_parent_task, &_on_accepted)
        end


        def stop
          Inproc.unbind(@endpoint)
        end

      end
    end
  end
end
