class ProxySQLChaosHelper
  class << self
    def with_connection_killer(conn, time_to_wait)
      Thread.new do
        sleep(time_to_wait)
        conn.close()
      end
      yield
    end

    def kill_host(name)
      raise "Could not delete MySQL sandbox with name #{name}" unless system("./dbdeployer delete #{name}")
    end
  end
end