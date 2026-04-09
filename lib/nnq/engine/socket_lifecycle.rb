# frozen_string_literal: true

module NNQ
  class Engine
    # Owns the socket-level state: `:new → :open → :closing → :closed`
    # and the captured parent task for the socket's task tree.
    #
    # Engine delegates state queries here and uses it to coordinate the
    # ordering of close-time side effects. Mirrors OMQ's SocketLifecycle
    # without the heartbeat/mechanism/monitor machinery nnq doesn't need.
    #
    class SocketLifecycle
      class InvalidTransition < RuntimeError; end

      STATES = %i[new open closing closed].freeze

      TRANSITIONS = {
        new:     %i[open closed].freeze,
        open:    %i[closing closed].freeze,
        closing: %i[closed].freeze,
        closed:  [].freeze,
      }.freeze


      # @return [Symbol]
      attr_reader :state

      # @return [Async::Task, nil] root of the socket's task tree
      attr_reader :parent_task

      # @return [Boolean] true if parent_task is the shared Reactor thread
      attr_reader :on_io_thread


      def initialize
        @state        = :new
        @parent_task  = nil
        @on_io_thread = false
      end


      def open?    = @state == :open
      def closing? = @state == :closing
      def closed?  = @state == :closed
      def alive?   = @state == :new || @state == :open


      # Captures +task+ as this socket's task tree root. Transitions
      # `:new → :open`. Idempotent: second call is a no-op.
      #
      # @param task [Async::Task]
      # @param on_io_thread [Boolean] true when +task+ is the shared
      #   NNQ::Reactor root task (vs. the caller's own Async task)
      # @return [Boolean] true on first-time capture, false if already captured
      def capture_parent_task(task, on_io_thread:)
        return false if @parent_task
        @parent_task  = task
        @on_io_thread = on_io_thread
        transition!(:open)
        true
      end


      # Transitions `:open → :closing`.
      def start_closing!
        transition!(:closing)
      end


      # Transitions `:closing → :closed` (or `:new → :closed` for
      # never-opened sockets).
      def finish_closing!
        transition!(:closed)
      end


      private

      def transition!(new_state)
        allowed = TRANSITIONS[@state]
        unless allowed&.include?(new_state)
          raise InvalidTransition, "#{@state} → #{new_state}"
        end
        @state = new_state
      end
    end
  end
end
