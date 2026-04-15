# frozen_string_literal: true

require "socket"
require "io/stream"

module NNQ
  module Transport
    # IPC transport using Unix domain sockets.
    #
    # Supports both file-based paths and Linux abstract namespace
    # (paths starting with @). Wire format is identical to TCP: SP/TCP
    # greeting followed by framed messages, so Protocol::SP handles it
    # verbatim.
    #
    module IPC
      class << self
        # Binds an IPC server.
        #
        # @param endpoint [String] e.g. "ipc:///tmp/nnq.sock" or "ipc://@abstract"
        # @param engine [Engine]
        # @return [Listener]
        def bind(endpoint, engine)
          path      = parse_path(endpoint)
          sock_path = to_socket_path(path)

          File.delete(sock_path) if !abstract?(path) && File.exist?(sock_path)

          server = UNIXServer.new(sock_path)
          Listener.new(endpoint, server, path, engine)
        end


        # Connects to an IPC endpoint.
        #
        # @param endpoint [String]
        # @param engine [Engine]
        # @return [void]
        def connect(endpoint, engine)
          path      = parse_path(endpoint)
          sock_path = to_socket_path(path)
          sock      = UNIXSocket.new(sock_path)
          engine.handle_connected(IO::Stream::Buffered.wrap(sock), endpoint: endpoint, framing: :ipc)
        end


        def parse_path(endpoint)
          endpoint.sub(%r{\Aipc://}, "")
        end


        # Converts @ prefix to \0 for Linux abstract namespace.
        def to_socket_path(path)
          abstract?(path) ? "\0#{path[1..]}" : path
        end


        def abstract?(path)
          path.start_with?("@")
        end
      end


      # A bound IPC listener.
      class Listener
        attr_reader :endpoint

        def initialize(endpoint, server, path, engine)
          @endpoint = endpoint
          @server   = server
          @path     = path
          @engine   = engine
          @task     = nil
        end


        def start_accept_loop(parent_task, &on_accepted)
          @task = parent_task.async(annotation: "nnq ipc accept #{@endpoint}") do
            loop do
              client = @server.accept
              # Engine's per-listener block closes over +framing+ below.
              on_accepted.call(IO::Stream::Buffered.wrap(client), :ipc)
            rescue Async::Stop, IOError
              break
            end
          ensure
            @server.close rescue nil
          end
        end


        def stop
          @task&.stop
          @server.close rescue nil
          File.delete(@path) rescue nil unless IPC.abstract?(@path)
        end

      end
    end
  end
end
