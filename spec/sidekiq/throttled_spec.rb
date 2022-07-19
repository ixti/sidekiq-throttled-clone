# frozen_string_literal: true

require "json"
begin
  require "sidekiq-pro"
  require "sidekiq/pro/super_fetch"
rescue LoadError
  true
end

RSpec.describe Sidekiq::Throttled, :sidekiq => :disabled do
  describe ".setup!" do
    before do
      # #setup! initializes the fetch strategy
      # and raises when queues is empty
      Sidekiq.options[:queues] = ["default"]

      require "sidekiq/processor"
      allow(Sidekiq).to receive(:server?).and_return true

      described_class.setup!
    end

    it "presets Sidekiq fetch strategy to Sidekiq::Throttled::Fetch" do
      expect(Sidekiq.options[:fetch]).to be_a Sidekiq::Throttled::Fetch
    end

    it "injects Sidekiq::Throttled::Middleware server middleware" do
      expect(Sidekiq.server_middleware.exists?(Sidekiq::Throttled::Middleware))
        .to be true
    end

    if Sidekiq.pro?
      context "with Sidekiq::Pro" do
        before do
          # Enable super_fetch if available; setup a weird call to expect for testing
          Sidekiq.super_fetch! do
            Kernel.exit
          end

          described_class.setup!
        end

        it "sets up orphan handling" do
          expect(described_class).to receive(:recover!).with("foo")
          expect(Kernel).to receive(:exit)

          Sidekiq.options[:fetcher].notify_orphan("foo")
        end
      end
    end
  end

  describe ".throttled?" do
    it "tolerates invalid JSON message" do
      expect(described_class.throttled?("][")).to be false
    end

    it "tolerates invalid (not fully populated) messages" do
      expect(described_class.throttled?(%({"class" => "foo"}))).to be false
    end

    it "tolerates if limiter not registered" do
      message = %({"class":"foo","jid":#{jid.inspect}})
      expect(described_class.throttled?(message)).to be false
    end

    it "passes JID to registered strategy" do
      strategy = Sidekiq::Throttled::Registry.add("foo",
        :threshold   => { :limit => 1, :period => 1 },
        :concurrency => { :limit => 1 })

      payload_jid = jid
      message     = %({"class":"foo","jid":#{payload_jid.inspect}})

      expect(strategy).to receive(:throttled?).with payload_jid

      described_class.throttled? message
    end

    it "unwraps ActiveJob-jobs default sidekiq adapter" do
      strategy = Sidekiq::Throttled::Registry.add("wrapped-foo",
        :threshold   => { :limit => 1, :period => 1 },
        :concurrency => { :limit => 1 })

      payload_jid = jid
      message     = JSON.dump({
        "class"   => "ActiveJob::QueueAdapters::SidekiqAdapter::JobWrapper",
        "wrapped" => "wrapped-foo",
        "jid"     => payload_jid
      })

      expect(strategy).to receive(:throttled?).with payload_jid

      described_class.throttled? message
    end

    it "unwraps ActiveJob-jobs custom sidekiq adapter" do
      strategy = Sidekiq::Throttled::Registry.add("JobClassName",
        :threshold   => { :limit => 1, :period => 1 },
        :concurrency => { :limit => 1 })

      payload_jid = jid
      message     = JSON.dump({
        "class"   => "ActiveJob::QueueAdapters::SidekiqCustomAdapter::JobWrapper",
        "wrapped" => "JobClassName",
        "jid"     => payload_jid
      })

      expect(strategy).to receive(:throttled?).with payload_jid

      described_class.throttled? message
    end
  end

  describe ".recover!" do
    it "tolerates invalid JSON message" do
      expect(described_class.throttled?("][")).to be false
    end

    it "tolerates invalid (not fully populated) messages" do
      expect(described_class.throttled?(%({"class" => "foo"}))).to be false
    end

    it "tolerates if limiter not registered" do
      message = %({"class":"foo","jid":#{jid.inspect}})
      expect(described_class.throttled?(message)).to be false
    end

    it "passes JID to registered strategy" do
      strategy = Sidekiq::Throttled::Registry.add("foo",
        :threshold   => { :limit => 1, :period => 1 },
        :concurrency => { :limit => 1 })

      payload_jid = jid
      message     = %({"class":"foo","jid":#{payload_jid.inspect}})

      expect(strategy).to receive(:finalize!).with payload_jid

      described_class.recover! message
    end
  end
end
