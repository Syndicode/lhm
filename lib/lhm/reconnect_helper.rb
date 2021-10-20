module Lhm
  module ReconnectHelper

    class ReconnectToHostSuccessful < Lhm::Error; end

    RECONNECT_SUCCESSFUL_MESSAGE = "LHM successfully reconnected to initial host:"
    ABNORMAL_EXECUTION_TIME_THRESHOLD = 5

    MYSQL_VAR_NAMES = {
      hostname: "@@global.hostname",
      server_uuid: "@@global.server_uuid",
      proxysql: "@@version_comment",
    }

    def initialize_reconnect_helper(connection, retry_config = {})
      @retry_config = retry_config
      @connection = connection
      @initial_server_uuid = server_uuid
      @initial_hostname = hostname

      # TODO: Run on every query or only once?
      @proxysql_enabled = proxysql?
    end

    # Complete explanation of algorithm: https://github.com/Shopify/db-engineering/issues/98#issuecomment-934948590
    def with_retries_and_reconnect
      Retriable.retriable(@retry_config) do
        raise Lhm::Error.new("Could not reconnected to initial MySQL host. Aborting to avoid data-loss") unless same_host_as_initial?
        yield
      rescue StandardError => e
        # Not all errors should trigger a reconnect. Some errors such be raised and abort the LHM (such as reconnecting to the wrong host).
        # Note: Will only try to reconnect if the required
        raise e unless error_can_trigger_reconnect?(e)
        reconnect_with_host_check!
      end
    end

    private

    def hostname
      mysql_single_value(MYSQL_VAR_NAMES[:hostname], @proxysql_enabled)
    end

    def server_uuid
      mysql_single_value(MYSQL_VAR_NAMES[:server_uuid], @proxysql_enabled)
    end

    def proxysql?
      mysql_single_value(MYSQL_VAR_NAMES[:proxysql]).include?("(ProxySQL)")
    end

    def mysql_single_value(name, with_lhm_annotations = false)
      query = "SELECT #{name} LIMIT 1"
      query = query.prepend("maintenance:lhm") if with_lhm_annotations

      @connection&.execute(query).to_a.first.tap do |record|
        return record&.first
      end
    end

    def same_host_as_initial?
      @initial_server_uuid == server_uuid
    end

    def reconnect_with_host_check!
      log_with_prefix("Lost connection to MySQL, will retry to connect to same host")
      begin
        Retriable.retriable(reconnect_retry_config) do
          # tries to reconnect. On failure will trigger a retry
          @connection.reconnect!
          if same_host_as_initial?
            # This is not an actual error, but it needs to trigger the Retriable
            # from #with_retries to execute the desired logic again
            raise ReconnectToHostSuccessful.new("LHM successfully reconnected to initial host: #{@initial_host} (server_uuid: #{@initial_server_id})")
          else
            # New Master --> abort LHM (reconnecting will not change anything)
            raise Lhm::Error.new("Reconnected to wrong host. Started migration on: #{@initial_host} (server_uuid: #{@initial_server_id}), but reconnected to: #{hostname} (server_uuid: #{@initial_server_id}).")
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

    def reconnect_retry_config
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