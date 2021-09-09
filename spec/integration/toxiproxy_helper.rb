# frozen_string_literal: true

class ToxiproxyHelper
  class << self
    Toxiproxy.host = 'http://toxiproxy.local:5665'

    Toxiproxy.populate([
      {
        name: 'shopify_test_mysql_writer',
        listen: '127.0.0.1:22220',
        upstream: '127.0.0.1:33006'
      },
      {
        name: 'shopify_test_mysql_reader',
        listen: '127.0.0.1:22221',
        upstream: '127.0.0.1:33007'
      },
      {
        name: 'shopify_test_proxysql',
        listen: '127.0.0.1:22222',
        upstream: '127.0.0.1:33005'
      }
    ])
  end
end
