require 'retriable'
require 'lhm/sql_helper'

module Lhm
  # SqlRetry standardizes the interface for retry behavior in components like
  # Entangler, AtomicSwitcher, ChunkerInsert.
  #
  # By default if an error includes the message "Lock wait timeout exceeded", or
  # "Deadlock found when trying to get lock", SqlRetry will retry again
  # once the MySQL client returns control to the caller, plus one second.
  # It will retry a total of 10 times and output to the logger a description
  # of the retry with error information, retry count, and elapsed time.
  #
  # This behavior can be modified by passing `options` that are documented in
  # https://github.com/kamui/retriable. Additionally, a "log_prefix" option,
  # which is unique to SqlRetry can be used to prefix log output.
  class SqlRetry
    RECONNECT_SUCCESSFUL_MESSAGE = "LHM successfully reconnected to initial host:"
    ABNORMAL_EXECUTION_TIME_THRESHOLD = 5

    # This internal error is used to trigger retries from the parent Retriable.retriable in #with_retries
    class ReconnectToHostSuccessful < Lhm::Error; end

    def initialize(connection, options = {}, with_consistent_host = false, retry_for_host_options = {})
      @connection = connection
      @log_prefix = options.delete(:log_prefix)
      @retry_config = default_retry_config.dup.merge!(options)
      if (@with_consistent_host = with_consistent_host)
        @host_retry_config = default_host_retry_config.dup.merge!(retry_for_host_options)
        @initial_host = hostname
        @initial_server_id = server_id
      end
    end

    # Complete explanation of algorithm: https://github.com/Shopify/db-engineering/issues/98#issuecomment-934948590
    def with_retries(override_prefix: nil)
      # Overrides log prefix if necessary or passed from parent to child instance (ex: Chunker -> ChunkInsert)
      old_prefix = @log_prefix
      @log_prefix = override_prefix unless override_prefix.nil?

      Retriable.retriable(retry_config) do
        if @with_consistent_host
          raise Lhm::Error.new("Could not reconnected to initial MySQL host. Aborting to avoid data-loss") unless same_host_as_initial?
        end
        # The time check will log a warning if an action (such as a MySQL query) takes more than 5s (there's a possibility a
        # failover might've happened after the hostname check above)
        with_time_check do
          yield(@connection)
        end
      rescue StandardError => e
        # Not all errors should trigger a reconnect. Some errors such be raised and abort the LHM (such as reconnecting to the wrong host).
        # Note: Will only try to reconnect if the required
        raise e unless @with_consistent_host && error_can_trigger_reconnect?(e)
        reconnect_with_host_check!
      end
    ensure
      # Restore the initial log prefix once outside of the block (ex: Chunker -> ChunkInsert).
      @log_prefix = old_prefix
    end

    attr_reader :retry_config

    private

    def with_time_check
      t_start = Time.new

      # Captures return value from the provided block
      return_value = yield

      if (Time.new - t_start) >= ABNORMAL_EXECUTION_TIME_THRESHOLD
        log_with_prefix("Query took abnormal amount of time to execute and the host check might not be accurate anymore", :warn)
      end

      return_value
    end

    def hostname
      # Context Should be defined by library caller
      @connection&.execute("SELECT @@global.hostname").to_a.first.tap do |record|
        return record&.first
      end
    end

    def server_id
      # This should only be used with CloudSQLs as their hosts are "localhost", but the "server_id" is different
      @connection&.execute("SELECT @@global.server_id").to_a.first.tap do |record|
        return record&.first
      end
    end

    def log_with_prefix(message, level = :info)
      message.prepend("[#{@log_prefix}] ") if @log_prefix
      Lhm.logger.send(level, message)
    end

    def reconnect_with_host_check!
      log_with_prefix("Lost connection to MySQL, will retry to connect to same host")
      begin
        Retriable.retriable(@host_retry_config) do
          # tries to reconnect. On failure will trigger a retry
          @connection.reconnect!
          new_host = hostname
          if new_host == @initial_host
            # This is not an actual error, but it needs to trigger the Retriable
            # from #with_retries to execute the desired logic again
            raise ReconnectToHostSuccessful.new("LHM successfully reconnected to initial host: #{@initial_host} (server_id: #{@initial_server_id})")
          else
            # New Master --> abort LHM (reconnecting will not change anything)
            raise Lhm::Error.new("Reconnected to wrong host. Started migration on: #{@initial_host} (server_id: #{@initial_server_id}), but reconnected to: #{new_host} (server_id: #{@initial_server_id}).")
          end
        end
      rescue StandardError => e
        # The parent Retriable.retriable is configured to retry if it encounters an error with the success message.
        # Therefore, if the connection is re-established successfully AND the host is the same, LHM can retry the query
        # that originally failed.
        raise e if reconnect_successful?(e)
        # If the connection was not successful, the parent retriable will raise "unregistered" errors.
        # Therefore, this error will cause the LHM to abort
        raise Lhm::Error.new("LHM tried the reconnection procedure but failed. Latest error: #{e.message}")
      end
    end

    def reconnect_successful?(e)
      e.class == ReconnectToHostSuccessful
    end

    def same_host_as_initial?
      host = hostname

      return server_id == @initial_server_id if host == "localhost"
      return host == @initial_host
    end

    # For a full list of configuration options see https://github.com/kamui/retriable
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
          if reconnect_successful?(exception)
            log_with_prefix("#{exception.message} -- triggering retry", :info)
          else
            log_with_prefix("#{exception.class}: '#{exception.message}' - #{try_number} tries in #{total_elapsed_time} seconds and #{next_interval} seconds until the next try.", :error)
          end
        end
      }.freeze
    end

    def error_can_trigger_reconnect?(err)
      err_msg = err.message
      regexes = [
        /Lost connection to MySQL server during query/,
        /MySQL client is not connected/,
        /Max connect timeout reached/,
        /Unknown MySQL server host/,
        /connection is locked to hostgroup/
      ]

      regexes.any? { |reg| err_msg.match(reg) }
    end

    def default_host_retry_config
      {
        on: {
          StandardError => [
            /Lost connection to MySQL server at 'reading initial communication packet'/
          ]
        },
        multiplier: 1, # each successive interval grows by this factor
        base_interval: 0.2, # the initial interval in seconds between tries.
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
