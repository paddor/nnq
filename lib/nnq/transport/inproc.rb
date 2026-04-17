# frozen_string_literal: true

require "async/queue"

require_relative "inproc/pipe"

module NNQ
  module Transport
    # In-process transport. Both peers live in the same process and
    # exchange frozen Strings through a pair of {Async::Queue}s — no
    # wire framing, no socketpair, no SP handshake.
    #
    # The historical implementation ran through a Unix `socketpair(2)`
    # and the full SP protocol, making inproc roughly as expensive as
    # IPC. Swapping to {Inproc::Pipe} (duck-types {NNQ::Connection})
    # drops the kernel buffer copy, the framing encode/decode, and the
    # handshake — inproc becomes a pure in-process queue transfer.
    #
    module Inproc
      Engine.transports["inproc"] = self

      @registry = {}
      @mutex    = Mutex.new


      class << self
        # Binds +engine+ to +endpoint+ in the process-global registry.
        #
        # @param endpoint [String] e.g. "inproc://my-endpoint"
        # @param engine [Engine]
        # @return [Listener]
        def bind(endpoint, engine, **)
          @mutex.synchronize do
            raise Error, "inproc endpoint already bound: #{endpoint}" if @registry.key?(endpoint)
            @registry[endpoint] = engine
          end

          Listener.new(endpoint)
        end


        # Connects +engine+ to a bound inproc endpoint. Creates a Pipe
        # pair — one queue per direction — and registers each side with
        # its owning engine via {Engine#connection_ready}. No handshake
        # runs; both ends are live as soon as the pipes are wired.
        #
        # @param endpoint [String]
        # @param engine [Engine]
        # @return [void]
        def connect(endpoint, engine, **)
          bound = @mutex.synchronize { @registry[endpoint] }
          raise Error, "inproc endpoint not bound: #{endpoint}" unless bound

          a_to_b = Async::Queue.new
          b_to_a = Async::Queue.new
          client = Pipe.new(send_queue: a_to_b, recv_queue: b_to_a, endpoint: endpoint)
          server = Pipe.new(send_queue: b_to_a, recv_queue: a_to_b, endpoint: endpoint)
          client.peer = server
          server.peer = client

          bound.connection_ready(server, endpoint: endpoint)
          engine.connection_ready(client, endpoint: endpoint)
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


        # No accept loop: inproc connects are fully synchronous.
        def start_accept_loop(_parent_task, &_on_accepted)
        end


        def stop
          Inproc.unbind(@endpoint)
        end

      end
    end
  end
end
