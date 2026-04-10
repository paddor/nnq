# frozen_string_literal: true

require "async/queue"
require_relative "backtrace"

module NNQ
  module Routing
    # RESPONDENT: reply side of the survey0 pattern.
    #
    # Semantics mirror REP: strict alternation of #receive then
    # #send_reply. The backtrace (survey ID + any hop IDs) is stripped
    # on receive and echoed verbatim on reply.
    #
    class Respondent
      include Backtrace

      def initialize(engine)
        @engine     = engine
        @recv_queue = Async::Queue.new
        @pending    = nil
        @mutex      = Mutex.new
      end


      # Receives the next survey body. Stashes the backtrace +
      # originating connection for the subsequent #send_reply.
      #
      # @return [String, nil] survey body, or nil if the socket was closed
      def receive
        @mutex.synchronize { @pending = nil }
        item = @recv_queue.dequeue
        return nil if item.nil?
        conn, btrace, body = item
        @mutex.synchronize { @pending = [conn, btrace] }
        body
      end


      # Sends +body+ as the reply to the most recently received survey.
      #
      # @param body [String]
      def send_reply(body)
        conn, btrace = @mutex.synchronize do
          raise Error, "RESPONDENT socket has no pending survey to reply to" unless @pending
          taken    = @pending
          @pending = nil
          taken
        end

        return if conn.closed?
        conn.send_message(btrace + body)
      end


      # Called by the engine recv loop with each received frame.
      def enqueue(body, conn)
        btrace, payload = parse_backtrace(body)
        return unless btrace # malformed/over-TTL — drop
        @recv_queue.enqueue([conn, btrace, payload])
      end


      def connection_removed(conn)
        @mutex.synchronize do
          @pending = nil if @pending && @pending[0] == conn
        end
      end


      def close
        @recv_queue.enqueue(nil)
      end


      def close_read
        @recv_queue.enqueue(nil)
      end
    end
  end
end
