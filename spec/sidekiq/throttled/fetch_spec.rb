# frozen_string_literal: true

require "sidekiq/api"
require "sidekiq/throttled/fetch"

begin
  require "sidekiq-pro"
  require "sidekiq/pro/super_fetch"
rescue LoadError
  true
end

require "support/working_class_hero"

RSpec.describe Sidekiq::Throttled::Fetch, :sidekiq => :disabled do
  subject(:fetcher) { described_class.new sidekiq_options }

  let(:options)       { { :queues => queues } }
  let(:queues)        { %w[heroes dreamers] }

  def sidekiq_options(**opts)
    sidekiq_options = Sidekiq.options = Sidekiq::DEFAULTS.merge(options.merge(opts))

    # Sidekiq 6.5 expects Sidekiq as a config object
    # https://github.com/mperham/sidekiq/commit/67daa7a408b214d593100f782271ed108686c147
    sidekiq_options = Sidekiq unless Gem::Version.new(Sidekiq::VERSION) < Gem::Version.new("6.5.0")

    # Set fetcher if not passed explicitly
    sidekiq_options[:fetcher] = sidekiq_options.fetch(:fetcher, Sidekiq::BasicFetch.new(sidekiq_options))
    sidekiq_options
  end

  describe ".new" do
    it "fails if :fetcher is missing" do
      opts = sidekiq_options
      if Gem::Version.new(Sidekiq::VERSION) < Gem::Version.new("6.5.0")
        opts.delete(:fetcher)
      else
        opts.options.delete(:fetcher)
      end
      expect { described_class.new opts }.to raise_error(KeyError, %r{:fetcher})
    end

    it "fails if :fetcher is nil" do
      options[:fetcher] = nil
      expect { fetcher }.to raise_error(ArgumentError, %r{:fetcher})
    end

    it "fails if :queues are empty" do
      options[:queues] = []
      expect { fetcher }.to raise_error(ArgumentError, %r{:queues})
    end

    it "is not strict by default" do
      options[:strict] = nil
      expect(fetcher.instance_variable_get(:@strict)).to be_falsy
    end

    it "cooldowns queues with TIMEOUT by default" do
      expect(Sidekiq::Throttled::ExpirableList)
        .to receive(:new)
        .with(described_class::TIMEOUT)
        .and_call_original
      fetcher
    end

    it "allows override throttled queues cooldown period" do
      expect(Sidekiq::Throttled::ExpirableList)
        .to receive(:new)
        .with(1312)
        .and_call_original

      options[:throttled_queue_cooldown] = 1312
      fetcher
    end
  end

  describe "#bulk_requeue" do
    before do
      Sidekiq::Client.push_bulk({
        "class" => WorkingClassHero,
        "args"  => Array.new(3) { [1, 2, 3] }
      })
    end

    let(:queue) { Sidekiq::Queue.new("heroes") }

    it "requeues" do
      works = Array.new(3) { fetcher.retrieve_work }
      expect(queue.size).to eq(0)

      fetcher.bulk_requeue(works, options)
      expect(queue.size).to eq(3)
    end
  end

  describe "#retrieve_work" do
    it "sleeps instead of BRPOP when queues list is empty" do
      fetcher.instance_variable_set(:@throttled_queues, %w[queue:heroes queue:dreamers])
      expect(fetcher.fetcher).to receive(:sleep).with(described_class::TIMEOUT)

      Sidekiq.redis do |redis|
        expect(redis).not_to receive(:brpop)
        expect(fetcher.retrieve_work).to be_nil
      end
    end

    context "when received job is throttled", :time => :frozen do
      before do
        Sidekiq::Client.push_bulk({
          "class" => WorkingClassHero,
          "args"  => Array.new(3) { [] }
        })
      end

      it "pauses job's queue for TIMEOUT seconds" do
        Sidekiq.redis do |redis|
          expect(Sidekiq::Throttled).to receive(:throttled?).and_return(true)
          expect(fetcher.retrieve_work).to be_nil

          expect(redis).to receive(:brpop)
            .with("queue:dreamers", 2)

          expect(fetcher.retrieve_work).to be_nil
        end
      end
    end

    shared_examples "expected behavior" do
      subject { fetcher.retrieve_work }

      before do
        Sidekiq::Client.push_bulk({
          "class" => WorkingClassHero,
          "args"  => Array.new(10) { [2, 3, 5] }
        })
      end

      it { is_expected.not_to be_nil }

      context "with strictly ordered queues" do
        before { options[:strict] = true }

        it "builds correct redis brpop command" do
          Sidekiq.redis do |conn|
            expect(conn).to receive(:brpop)
              .with("queue:heroes", "queue:dreamers", 2)
            fetcher.retrieve_work
          end
        end
      end

      context "with weight-ordered queues" do
        before { options[:strict] = false }

        it "builds correct redis brpop command" do
          Sidekiq.redis do |conn|
            queue_regexp = %r{^queue:(heroes|dreamers)$}
            expect(conn).to receive(:brpop).with(queue_regexp, queue_regexp, 2)
            fetcher.retrieve_work
          end
        end
      end

      context "when limit is not yet reached" do
        before { 3.times { fetcher.retrieve_work } }

        it { is_expected.not_to be_nil }
      end

      context "when limit exceeded" do
        before { 5.times { fetcher.retrieve_work } }

        it { is_expected.to be_nil }

        it "pushes fetched job back to the queue" do
          Sidekiq.redis do |conn|
            expect(conn).to receive(:lpush)
            fetcher.retrieve_work
          end
        end
      end
    end

    context "with static configuration" do
      before do
        WorkingClassHero.sidekiq_throttle(:threshold => {
          :limit  => 5,
          :period => 10
        })
      end

      include_examples "expected behavior"
    end

    context "with dynamic configuration" do
      before do
        WorkingClassHero.sidekiq_throttle(:threshold => {
          :limit  => ->(a, b, _) { a + b },
          :period => ->(a, b, c) { a + b + c }
        })
      end

      include_examples "expected behavior"
    end
  end

  if Sidekiq.pro?
    context "with SuperFetch" do
      subject(:fetcher) { described_class.new sidekiq_options(:fetcher => super_fetch) }

      let(:super_fetch) { Sidekiq::Pro::SuperFetch.new(sidekiq_options).tap(&:startup) }
      let(:queues) { %w[heroes] }
      let(:queue) { Sidekiq::Queue.new("heroes") }

      before do
        Sidekiq::Throttled.configuration.enhanced_queues = false
        Sidekiq::Client.push_bulk({
          "class" => WorkingClassHero,
          "args"  => Array.new(3) { [1, 2, 3] }
        })
      end

      describe ".new" do
        it "uses SuperFetcher" do
          expect(fetcher.fetcher).to eq(super_fetch)
        end
      end

      describe "#bulk_requeue" do
        it "requeues using rpoplpush" do
          works = Array.new(3) { fetcher.retrieve_work }
          expect(queue.size).to eq(0)

          Sidekiq.redis do |conn|
            expect(conn)
              .to receive(:rpoplpush).with(%r{queue:sq|.*|heroes}, "queue:heroes")
              .exactly(4).times
              .and_call_original
          end

          fetcher.bulk_requeue(works, sidekiq_options)

          expect(queue.size).to eq(3)
        end
      end

      describe "#retrieve_work" do
        it "retrieves work using brpoplpush" do
          Sidekiq.redis do |conn|
            expect(conn)
              .to receive(:brpoplpush).with("queue:heroes", %r{queue:sq|.*|heroes}, 1)
              .and_call_original
          end
          fetcher.retrieve_work
        end

        it "requeues using the super_requeue script" do
          Sidekiq::Pro::Scripting.bootstrap
          script_sha = Sidekiq::Pro::Scripting::SHAS[:super_requeue]
          Sidekiq.redis do |conn|
            allow(Sidekiq::Throttled).to receive(:throttled?).and_return(true)
            expect(conn)
              .to receive(:evalsha)
              .with(script_sha, anything, anything)
              .once
              .and_call_original
          end

          fetcher.retrieve_work
        end
      end
    end
  end
end
