# Creates replication user in Writer
CREATE USER IF NOT EXISTS 'shopify_writer'@'%' IDENTIFIED BY 'password';
CREATE USER IF NOT EXISTS 'shopify_reader'@'%' IDENTIFIED BY 'password';

CREATE USER IF NOT EXISTS 'replication'@'%' IDENTIFIED BY 'password';
GRANT REPLICATION SLAVE ON *.* TO' replication'@'%' IDENTIFIED BY 'password';

# Apply privileges
FLUSH PRIVILEGES;
