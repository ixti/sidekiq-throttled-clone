# frozen_string_literal: true

module Sidekiq
  module Throttled
    module Patches
      module BasicFetch
        module RequeueThrottled
          def requeue_throttled
            Sidekiq.redis { |conn| conn.lpush(queue, job) }
          end
        end

        # For compatability with pausable queues
        module PausableQueues
          def initialize(options)
            @paused = Set.new
            super
          end

          def notify(action, queue)
            case action
            when :pause
              @paused << queue
            when :unpause
              @paused.delete queue
            end
          end

          def queues_cmd
            super - @paused.to_a
          end
        end

        # Backport of a fix introduced in 6.2 for compatability
        module RetrieveWork
          def retrieve_work
            qs = queues_cmd

            # 4825 Sidekiq Pro with all queues paused will return an
            # empty set of queues with a trailing TIMEOUT value.
            if qs.size <= 1
              sleep(Throttled::Fetch::TIMEOUT)
              return nil
            end

            super
          end
        end

        def self.apply!
          require "sidekiq/fetch"
          ::Sidekiq::BasicFetch.prepend(RetrieveWork) if Gem::Version.new(Sidekiq::VERSION) < Gem::Version.new("6.2.0")
          ::Sidekiq::BasicFetch.prepend(PausableQueues) unless ::Sidekiq::BasicFetch.method_defined?(:notify)

          ::Sidekiq::BasicFetch::UnitOfWork.include(RequeueThrottled)
        end
      end
    end
  end
end

Sidekiq::Throttled::Patches::BasicFetch.apply!
