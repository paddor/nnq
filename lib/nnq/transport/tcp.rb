# frozen_string_literal: true

require "socket"
require "uri"
require "io/stream"

module NNQ
  module Transport
    # TCP transport. Smaller than omq's: no IPv6 dual-bind dance, no
    # custom buffer-size sockopts (yet). One server per bind, blocking
    # accept inside an Async fiber.
    #
    module TCP
      class << self
        # Binds a TCP server to +endpoint+.
        #
        # @param endpoint [String] e.g. "tcp://127.0.0.1:5570" or "tcp://127.0.0.1:0"
        # @param engine [Engine]
        # @return [Listener]
        def bind(endpoint, engine)
          host, port = parse_endpoint(endpoint)
          host = "0.0.0.0" if host == "*"
          server = TCPServer.new(host, port)
          actual = server.local_address.ip_port
          host_part = host.include?(":") ? "[#{host}]" : host
          Listener.new("tcp://#{host_part}:#{actual}", server, actual, engine)
        end


        # Connects to +endpoint+ and registers the resulting pipe with
        # the engine. Synchronous (errors propagate to the caller).
        #
        # @param endpoint [String]
        # @param engine [Engine]
        # @return [void]
        def connect(endpoint, engine)
          host, port = parse_endpoint(endpoint)
          sock = TCPSocket.new(host, port)
          engine.handle_connected(IO::Stream::Buffered.wrap(sock), endpoint: endpoint)
        end


        def parse_endpoint(endpoint)
          uri = URI.parse(endpoint)
          [uri.hostname, uri.port]
        end
      end


      # A bound TCP listener.
      #
      class Listener
        attr_reader :endpoint
        attr_reader :port

        def initialize(endpoint, server, port, engine)
          @endpoint = endpoint
          @server   = server
          @port     = port
          @engine   = engine
          @task     = nil
        end


        # Spawns an accept loop fiber under +parent_task+ that yields
        # IO::Stream::Buffered for each accepted connection.
        def start_accept_loop(parent_task, &on_accepted)
          @task = parent_task.async(annotation: "nnq tcp accept #{@endpoint}") do
            loop do
              client = @server.accept
              on_accepted.call(IO::Stream::Buffered.wrap(client))
            rescue Async::Stop
              break
            rescue IOError
              break
            end
          ensure
            @server.close rescue nil
          end
        end


        def stop
          @task&.stop
          @server.close rescue nil
        end
      end
    end
  end
end
