# frozen_string_literal: true

# 3rd party
require "sidekiq"

# internal
require "sidekiq/throttled/version"
require "sidekiq/throttled/configuration"
require "sidekiq/throttled/registry"
require "sidekiq/throttled/job"
require "sidekiq/throttled/worker"
require "sidekiq/throttled/utils"

# @see https://github.com/mperham/sidekiq/
module Sidekiq
  # Concurrency and threshold throttling for Sidekiq.
  #
  # Just add somewhere in your bootstrap:
  #
  #     require "sidekiq/throttled"
  #     Sidekiq::Throttled.setup!
  #
  # Once you've done that you can include {Sidekiq::Throttled::Job} to your
  # job classes and configure throttling:
  #
  #     class MyJob
  #       include Sidekiq::Job
  #       include Sidekiq::Throttled::Job
  #
  #       sidekiq_options :queue => :my_queue
  #
  #       sidekiq_throttle({
  #         # Allow maximum 10 concurrent jobs of this class at a time.
  #         :concurrency => { :limit => 10 },
  #         # Allow maximum 1K jobs being processed within one hour window.
  #         :threshold => { :limit => 1_000, :period => 1.hour }
  #       })
  #
  #       def perform
  #         # ...
  #       end
  #     end
  module Throttled
    MUTEX = Mutex.new
    private_constant :MUTEX

    class << self
      include Utils

      # @return [Configuration]
      def configuration
        @configuration ||= Configuration.new
      end

      # Hooks throttler into sidekiq.
      #
      # @param options [Hash]
      # @option options [Fetch] :fetcher (nil)
      #   Sidekiq fetcher to use for fetching jobs.
      # @option options [Boolean] :enhanced_queues (true)
      #   Enabled pausable queues & enhanced queues tab in Web.
      # @return [void]
      def setup!(fetcher: Sidekiq.options[:fetch])
        Sidekiq.configure_server do |config|
          config.options[:fetcher] = fetcher

          setup_orphan_handling!(fetcher) if Sidekiq.pro?
          setup_strategy!(config)

          require "sidekiq/throttled/middleware"
          config.server_middleware do |chain|
            chain.add Sidekiq::Throttled::Middleware
          end
        end
      end

      # Tells whenever job is throttled or not.
      #
      # @param [String] message Job's JSON payload
      # @return [Boolean]
      def throttled?(message)
        with_strategy_and_job(message) do |strategy, jid, args|
          return strategy.throttled? jid, *args
        end

        false
      rescue
        false
      end

      # Manual reset throttle for job that had been orphaned.
      #
      # @param [String] message Job's JSON payload
      # @return [Void]
      def recover!(message)
        with_strategy_and_job(message) do |strategy, jid, args|
          strategy.finalize! jid, *args
        end
      end

      private

      # @return [void]
      def setup_strategy!(sidekiq_config)
        require "sidekiq/throttled/fetch"

        # https://github.com/mperham/sidekiq/commit/67daa7a408b214d593100f782271ed108686c147
        sidekiq_config = sidekiq_config.options if Gem::Version.new(Sidekiq::VERSION) < Gem::Version.new("6.5.0")

        sidekiq_config[:fetcher] ||= Sidekiq::BasicFetch.new(sidekiq_config)
        sidekiq_config[:fetch] = Sidekiq::Throttled::Fetch.new(sidekiq_config)
      end

      # @return [void]
      def setup_orphan_handling!(fetcher)
        # skip if not supported
        return unless fetcher.respond_to?(:orphan_handler=)

        # Replace the existing orphan handler & call it after calling recover!
        original_handler = fetcher.orphan_handler
        fetcher.orphan_handler = proc do |msg, pill|
          recover!(msg)
          original_handler.call(msg, pill) if original_handler
        end
      end

      # @return [Void]
      def with_strategy_and_job(message)
        message = JSON.parse message
        job = message.fetch("wrapped") { message.fetch("class") { return false } }
        jid = message.fetch("jid") { return false }

        preload_constant! job

        strategy = Registry.get job

        yield(strategy, jid, message["args"])
      end

      # Tries to preload constant by it's name once.
      #
      # Somehow, sometimes, some classes are not eager loaded upon Rails init,
      # leading to throttling config not being registered prior job perform.
      # And that leaves us with concurrency limit + 1 situation upon Sidekiq
      # server restart (becomes normal after all Sidekiq processes handled
      # at leas onr job of that class).
      #
      # @return [void]
      def preload_constant!(job)
        MUTEX.synchronize do
          @preloaded      ||= {}
          @preloaded[job] ||= constantize(job) || true
        end
      end
    end
  end
end
