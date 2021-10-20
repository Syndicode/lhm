require "lhm/reconnect_helper"

module Lhm
  class Connection
    include ReconnectHelper

    def initialize(ar_connection:, default_log_prefix: nil, reconnect: false, retry_options: {})
      @default_log_prefix = default_log_prefix
      @connection = ar_connection
      @logger = logger
      @retry_options = retry_options || default_retry_config
      if (@with_reconnect = reconnect)
        initialize_reconnect_helper(@connection, @retry_options)
      end
    end

    def with_log_prefix(override_prefix:)
      @old_prefix = @log_prefix
      @log_prefix = override_prefix
      yield
    ensure
      @log_prefix = @old_prefix
    end

    def query(query)
      @log_prefix = log_prefix || file
      if @with_reconnect
        with_retries_and_reconnect do
          @connection.execute(query)
        end
      else
        with_retries do
          @connection.execute(query)
        end
      end
    end

    private

    def with_retries
      Retriable.retriable(@retry_options) do
        yield
      end
    end

    def log_with_prefix(message, level = :info)
      message.prepend("[#{@log_prefix}] ") if @log_prefix
      Lhm.logger.send(level, message)
    end

    # returns humanized file of caller
    def file
      # check order
      /[\/]?(\w+.rb):\d+:in `\w+'/.match(caller.third)
      name = $1.remove(".rb").humanize
      "#{name}"
    end

    def default_retry_config
      {
        on: {
          StandardError => [
            /Lock wait timeout exceeded/,
            /Timeout waiting for a response from the last query/,
            /Deadlock found when trying to get lock/,
            /Query execution was interrupted/,
            /Lost connection to MySQL server during query/,
            /Max connect timeout reached/,
            /Unknown MySQL server host/,
            /connection is locked to hostgroup/,
            /The MySQL server is running with the --read-only option so it cannot execute this statement/,
          ],
          ReconnectToHostSuccessful => [
            /#{RECONNECT_SUCCESSFUL_MESSAGE}/
          ]
        },
        multiplier: 1, # each successive interval grows by this factor
        base_interval: 1, # the initial interval in seconds between tries.
        tries: 20, # Number of attempts to make at running your code block (includes initial attempt).
        rand_factor: 0, # percentage to randomize the next retry interval time
        max_elapsed_time: Float::INFINITY, # max total time in seconds that code is allowed to keep being retried
        on_retry: Proc.new do |exception, try_number, total_elapsed_time, next_interval|
          log_with_prefix("#{exception.class}: '#{exception.message}' - #{try_number} tries in #{total_elapsed_time} seconds and #{next_interval} seconds until the next try.", :error)
        end
      }.freeze
    end
  end
end