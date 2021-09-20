class ProxySQLHelper
  class << self
    # Flips the destination hostgroup for /maintenance:lhm/ from 0 (i.e. writer) to 1 (i.e. reader)
    def with_lhm_hostgroup_flip
      conn = Mysql2::Client.new(
        host: '127.0.0.1',
        username: "remote-admin",
        password: "password",
        port: "6032",
      )

      begin
        conn.query("UPDATE mysql_query_rules SET destination_hostgroup=1 WHERE match_pattern=\"maintenance:lhm\"")
        conn.query("LOAD MYSQL QUERY RULES TO RUNTIME;")
        yield
      ensure
        conn.query("UPDATE mysql_query_rules SET destination_hostgroup=0 WHERE match_pattern=\"maintenance:lhm\"")
        conn.query("LOAD MYSQL QUERY RULES TO RUNTIME;")
      end
    end
  end
end
