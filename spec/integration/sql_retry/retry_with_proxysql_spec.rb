require 'minitest/autorun'
require 'mysql2'
require 'integration/sql_retry/lock_wait_timeout_test_helper'
require 'integration/sql_retry/proxysql_chaos_helper'
require 'lhm'

describe Lhm::SqlRetry, "ProxiSQL tests for LHM retry" do

  before(:each) do
    #TODO fix this with proxysql
    @old_logger = Lhm.logger
    @logger = StringIO.new
    Lhm.logger = Logger.new(@logger)

    @helper = LockWaitTimeoutTestHelper.new(
      lock_duration: 2,
      innodb_lock_wait_timeout: 2
    )

    @connection = Mysql2::Client(
      username: "proxysql",
      password: "proxysql",
      socket: "path/to/socket"
    )

    @helper.create_table_to_lock

    # Start a thread to hold a lock on the table
    @locked_record_id = @helper.hold_lock

    # Assert our pre-conditions
    assert_equal 2, @helper.record_count
  end

  after(:each) do
    # Restore default logger
    Lhm.logger = @old_logger
  end

  it "Will retry until connection with previous host is achieved" do
    lhm_retry = Lhm::SqlRetry.new(@connection)


    ProxySQLChaosHelper.with_connection_killer(@connection, 1.second) do
      lhm_retry.with_retries do |c|
      end
    end
  end

  it "Will abort LHM if it cannot get the same host" do

  end

end