# frozen_string_literal: true

require "async"

module NNQ
  # Per-process fallback IO thread for non-Async callers.
  #
  # When user code already runs inside an Async reactor, NNQ tasks attach
  # directly to the caller's task tree. When the caller is bare (e.g. a
  # plain `Thread.new` or the main thread of a script), NNQ::Reactor lazily
  # spawns one shared background thread that hosts an Async reactor and
  # processes work items dispatched via {.run}.
  #
  # This is *not* an Async scheduler — it is a fallback thread for an
  # Async reactor. NNQ and OMQ each have their own private fallback for
  # bare-thread callers; both can coexist with the user's own reactor with
  # no extraction or sharing required.
  #
  module Reactor
    @mutex      = Mutex.new
    @thread     = nil
    @root_task  = nil
    @work_queue = nil


    class << self
      def root_task
        return @root_task if @root_task

        @mutex.synchronize do
          return @root_task if @root_task

          ready       = Thread::Queue.new
          @work_queue = Async::Queue.new
          @thread     = Thread.new { run_reactor(ready) }
          @thread.name = "nnq-io"
          @root_task  = ready.pop
          at_exit { stop! }
        end

        @root_task
      end


      def run(&block)
        if Async::Task.current?
          yield
        else
          result = Thread::Queue.new # FIXME: use Async::Promise (see OMQ)
          root_task # ensure started
          @work_queue.push([block, result])
          status, value = result.pop
          raise value if status == :error
          value
        end
      end


      def stop!
        return unless @thread&.alive?
        @work_queue&.push(nil)
        @thread&.join(2)
        @thread     = nil
        @root_task  = nil
        @work_queue = nil
      end


      private


      def run_reactor(ready)
        Async do |task|
          ready.push(task)

          loop do
            item = @work_queue.dequeue
            break if item.nil?
            block, result = item
            task.async do
              result.push([:ok, block.call])
            rescue => e
              result.push([:error, e])
            end
          end
        end
      end

    end
  end
end
