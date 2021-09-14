require 'yaml'
require 'mysql2'

class DBConnectionHelper

  DATABASE_CONFIG_FILE = "database-new.yml"

  class << self
    def db_config
      @db_config ||= YAML.load_file(File.expand_path(File.dirname(__FILE__)) + "/../#{DATABASE_CONFIG_FILE}")
    end

    def new_mysql_connection(role= :master, with_data= false)
      conn = case role
             when :master
               new_mysql_connection_master
             when :slave
               new_mysql_connection_slave
             when :proxysql
               new_mysql_connection_proxysql
             end
      init_test_db(conn)
      init_with_dummy_data(conn) if with_data
      conn
    end

    def new_mysql_connection_master
      ActiveRecord::Base.establish_connection(
        :host => '127.0.0.1',
        :username => db_config['master']['user'],
        :password => db_config['master']['password'],
        :port => db_config['master']['port']
      )
    end

    def new_mysql_connection_proxysql
      ActiveRecord::Base.establish_connection(
        :host => '127.0.0.1',
        :username => db_config['proxysql']['user'],
        :password => db_config['proxysql']['password'],
        :port => db_config['proxysql']['port']
      )
    end

    def new_mysql_connection_slave
      ActiveRecord::Base.establish_connection(
        :host => '127.0.0.1',
        :username => db_config['slave']['user'],
        :password => db_config['slave']['password'],
        :port => db_config['slave']['port']
      )
    end

    def test_db_name
      @test_db_name ||= "test"
    end

    def test_table_name
      @test_table_name ||= "test"
    end

    def init_test_db(client)
      # For some reasons sometimes the database does not exist
      client.execute("CREATE DATABASE IF NOT EXISTS #{test_db_name}")
      client.execute("USE #{test_db_name}")
    end

    def init_with_dummy_data(conn)
      conn.execute("DROP TABLE IF EXISTS #{test_table_name} ")
      conn.execute("CREATE TABLE #{test_table_name} (a varchar(10), b varchar(10), c varchar(10))")

      1.upto(9) do |i|
        val = "#{i}" * 10
        query = "INSERT INTO #{test_table_name} VALUES(\"#{val}\",\"#{val}\",\"#{val}\")"
        conn.execute(query)
      end
    end
  end
end