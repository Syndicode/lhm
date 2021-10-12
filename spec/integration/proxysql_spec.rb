describe "ProxySQL integration" do
  it "Should contact the writer" do
    conn = Mysql2::Client.new(
      host: '127.0.0.1',
      username: "writer",
      password: "password",
      port: "33005",
      )

    assert_equal conn.query("SELECT @@global.hostname as host").each.first["host"], "mysql-1"
  end

  it "Should contact the reader" do
    conn = Mysql2::Client.new(
      host: '127.0.0.1',
      username: "reader",
      password: "password",
      port: "33005",
      )

    assert_equal conn.query("SELECT @@global.hostname as host").each.first["host"], "mysql-2"
  end

  it "Should override default hostgroup from user if rule matches" do
    conn = Mysql2::Client.new(
      host: '127.0.0.1',
      username: "reader",
      password: "password",
      port: "33005",
      )

    assert_equal conn.query("/*maintenance:lhm*/SELECT @@global.hostname as host").each.first["host"], "mysql-1"
  end
end