require '/Users/mananmaniyar/src/github.com/Shopify/lhm/lib/lhm/timestamp'
require '/Users/mananmaniyar/src/github.com/Shopify/lhm/lib/lhm/sql_retry'

module Lhm
  module Cleanup
    class Current
      def initialize(run, origin_table_name, connection, options = {})
        @run = run
        @table_name = TableName.new(origin_table_name)
        @connection = connection
        @ddls = []
        @retry_helper = SqlRetry.new(
          @connection,
          {
            log_prefix: "Cleanup::Current"
          }.merge!(options.fetch(:retriable, {}))
        )
      end

      attr_reader :run, :connection, :ddls

      def execute
        build_statements_for_drop_lhm_triggers_for_origin
        build_statements_for_rename_lhmn_tables_for_origin
        if run
          execute_ddls
        else
          report_ddls
        end
      end

      private

      def build_statements_for_drop_lhm_triggers_for_origin
        lhm_triggers_for_origin.each do |trigger|
          @ddls << "drop trigger if exists #{trigger}"
        end
      end

      def lhm_triggers_for_origin
        @lhm_triggers_for_origin ||= all_triggers_for_origin.select { |name| name =~ /^lhmt/ }
      end

      def all_triggers_for_origin
        @all_triggers_for_origin ||= connection.select_values("show triggers like '%#{@table_name.original}'").collect do |trigger|
          trigger.respond_to?(:trigger) ? trigger.trigger : trigger
        end
      end

      def build_statements_for_rename_lhmn_tables_for_origin
        lhmn_tables_for_origin.each do |table|
          @ddls << "rename table #{table} to #{@table_name.failed}"
        end
      end

      def lhmn_tables_for_origin
        @lhmn_tables_for_origin ||= connection.select_values("show tables like '#{@table_name.new}'")
      end

      def execute_ddls
        ddls.each do |ddl|
          @retry_helper.with_retries do |retriable_connection|
            retriable_connection.execute(ddl)
          end
        end
        Lhm.logger.info("Dropped triggers on #{@lhm_triggers_for_origin.join(', ')}")
        Lhm.logger.info("Dropped tables #{@lhm_triggers_for_origin.join(', ')}")
      end

      def report_ddls
        Lhm.logger.info("The following DDLs would be executed: #{ddls}")
      end
    end
  end
end
