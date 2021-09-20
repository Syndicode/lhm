require 'yaml'
require 'mysql2'

class DBConnectionHelper

  DATABASE_CONFIG_FILE = "database-new.yml"

  class << self
    def db_config
      @db_config ||= YAML.load_file(File.expand_path(File.dirname(__FILE__)) + "/../#{DATABASE_CONFIG_FILE}")
    end

    def new_mysql_connection(role = :master, with_data = false, toxic = false)

      key = role.to_s + toxic_postfix(toxic)

      conn = ActiveRecord::Base.establish_connection(
        :host => '127.0.0.1',
        :adapter => "mysql2",
        :username => db_config[key]['user'],
        :password => db_config[key]['password'],
        :database => test_db_name,
        :port => db_config[key]['port']
      )
      conn = conn.connection
      init_test_db(conn)
      init_with_dummy_data(conn) if with_data
      conn
    end

    def toxic_postfix(toxic)
      toxic ? "_toxic" : ""
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
    end

    def init_with_dummy_data(conn)
      conn.execute("DROP TABLE IF EXISTS #{test_table_name} ")
      conn.execute("CREATE TABLE #{test_table_name} (id int)")

      1.upto(9) do |i|
        query = "INSERT INTO #{test_table_name} (id) VALUE (#{i})"
        conn.execute(query)
      end
    end
  end
end