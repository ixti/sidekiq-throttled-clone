# frozen_string_literal: true

require "sidekiq"
require "sidekiq/fetch"
require "sidekiq/throttled/expirable_list"
require "sidekiq/throttled/patches/basic_fetch"
require "sidekiq/throttled/queue_name"

module Sidekiq
  module Throttled
    # Throttled fetch strategy.
    #
    # @private
    class Fetch
      # Timeout to sleep between fetch retries in case of no job received,
      # as well as timeout to wait for redis to give us something to work.
      TIMEOUT = 2

      attr_reader :fetcher

      # Initializes fetcher instance.
      # @param options [Hash]
      # @option options [Integer] :throttled_queue_cooldown (TIMEOUT)
      #   Min delay in seconds before queue will be polled again after
      #   throttled job.
      # @option options [Boolean] :strict (false)
      # @option options [Array<#to_s>] :queue
      # @option options [Fetch] :fetcher
      def initialize(options)
        @throttled_queues = ExpirableList.new(options.fetch(:throttled_queue_cooldown, TIMEOUT))

        @fetcher = options.fetch(:fetcher)
        @strict = options.fetch(:strict, false)
        @queues = options.fetch(:queues).map { |q| QueueName.expand q }

        raise ArgumentError, "empty :queues" if @queues.empty?
        raise ArgumentError, ":fetcher not set" if @fetcher.nil?

        @queues.uniq! if @strict
      end

      # Retrieves job from original fetcher.
      #
      # @return [Sidekiq::*::UnitOfWork, nil]
      def retrieve_work
        work = without_queues(@throttled_queues) do
          fetcher.retrieve_work
        end

        return work unless work && Throttled.throttled?(work.job)

        work.respond_to?(:requeue_throttled) ? work.requeue_throttled : work.requeue
        @throttled_queues << QueueName.expand(work.queue_name)

        nil
      end

      # Requeues all given units as a single operation.
      #
      # @param [Array<Fetch::UnitOfWork>] units
      # @return [void]
      def bulk_requeue(units, options)
        fetcher.bulk_requeue(units, options)
      end

      private

      # Executes block without the passed queues active on the orignal fetcher
      # Compatible with BasicFetch & SuperFetch
      #
      # @param [Array<String>] queues
      #   The queues that should be active for this block
      def without_queues(queues, &_block)
        queues.each { |q| fetcher.notify(:pause, q) }
        yield
      ensure
        queues.each { |q| fetcher.notify(:unpause, q) }
      end
    end
  end
end
