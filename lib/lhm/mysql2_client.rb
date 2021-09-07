module Mysql2
  class Client
    def reconnect!
      raise Mysql2::Error "Connection is not closed yet. Try closing the connection before reconnecting" unless closed?

      opts = @query_options
      user     = opts[:username] || opts[:user]
      pass     = opts[:password] || opts[:pass]
      host     = opts[:host] || opts[:hostname]
      port     = opts[:port]
      database = opts[:database] || opts[:dbname] || opts[:db]
      socket   = opts[:socket] || opts[:sock]
      flags = opts[:flags] || 0
      conn_attrs = opts[:connect_attrs] || {:program_name => $PROGRAM_NAME}

      connect user, pass, host, port, database, socket, flags, conn_attrs
    end
  end
end