module Lhm
  module Throttler
    class ThreadsRunning
      include Command

      DEFAULT_STRIDE = 2_000
      DEFAULT_INITIAL_TIMEOUT = 0.1
      DEFAULT_HEALTHY_RANGE = (0..50)

      attr_accessor :timeout_seconds, :healthy_range, :connection
      attr_accessor :stride
      attr_reader :max_timeout_seconds, :initial_timeout_seconds

      def initialize(options = {})
        @initial_timeout_seconds = options[:initial_timeout] || DEFAULT_INITIAL_TIMEOUT
        @stride = options[:stride] || DEFAULT_STRIDE
        @max_timeout_seconds = options[:max_timeout] || (@initial_timeout_seconds * 1024)
        @timeout_seconds = @initial_timeout_seconds
        @healthy_range = options[:healthy_range] || DEFAULT_HEALTHY_RANGE
        @connection = options[:connection]
      end

      def threads_running
        query = <<~SQL.squish
              SELECT COUNT(*) as Threads_running
              FROM (
                SELECT 1 FROM performance_schema.threads
                WHERE NAME='thread/sql/one_connection'
                  AND PROCESSLIST_STATE IS NOT NULL
                LIMIT #{@healthy_range.max + 1}
              ) AS LIM
        SQL

        @connection.select_value(query)
      end

      def throttle_seconds
        current_threads_running = threads_running

        if !healthy_range.cover?(current_threads_running) && @timeout_seconds < @max_timeout_seconds
          Lhm.logger.info("Increasing timeout between strides from #{@timeout_seconds} to #{@timeout_seconds * 2} because threads running is greater than the maximum of #{@healthy_range.max} allowed.")
          @timeout_seconds = @timeout_seconds * 2
        elsif healthy_range.cover?(current_threads_running) && @timeout_seconds > @initial_timeout_seconds
          Lhm.logger.info("Decreasing timeout between strides from #{@timeout_seconds} to #{@timeout_seconds / 2} because threads running is less than the maximum of #{@healthy_range.max} allowed.")
          @timeout_seconds = @timeout_seconds / 2
        else
          @timeout_seconds
        end
      end

      def execute
        sleep throttle_seconds
      end
    end
  end
end
