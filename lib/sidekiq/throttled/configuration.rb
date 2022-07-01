# frozen_string_literal: true

module Sidekiq
  module Throttled
    # Configuration holder.
    class Configuration
      # Class constructor.
      def initialize
        reset!
      end

      # Reset configuration to defaults.
      #
      # @return [self]
      def reset!
        @inherit_strategies = false
        @enhanced_queues = true

        self
      end

      # Instructs throttler to lookup strategies in parent classes, if there's
      # no own strategy:
      #
      #     class FooJob
      #       include Sidekiq::Job
      #       include Sidekiq::Throttled::Job
      #
      #       sidekiq_throttle :concurrency => { :limit => 42 }
      #     end
      #
      #     class BarJob < FooJob
      #     end
      #
      # By default in the example above, `Bar` won't have throttling options.
      # Set this flag to `true` to enable this lookup in initializer, after
      # that `Bar` will use `Foo` throttling bucket.
      def inherit_strategies=(value)
        @inherit_strategies = value ? true : false
      end

      # Whenever throttled workers should inherit parent's strategies or not.
      # Default: `false`.
      #
      # @return [Boolean]
      def inherit_strategies?
        @inherit_strategies
      end

      # Instructs throttler to enable pausable queues and add the enhanced
      # queues tab to the web interface when sidekiq/throttled/web is required.
      def enhanced_queues=(value)
        @enhanced_queues = value ? true : false
      end

      # Wether pausable queues should be enabled
      # Default: `true`.
      #
      # @return [Boolean]
      def enhanced_queues?
        @enhanced_queues
      end
    end
  end
end
