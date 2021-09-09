# Creates replication user in Writer
CREATE USER IF NOT EXISTS 'replication'@'%' IDENTIFIED BY 'password';
GRANT REPLICATION SLAVE ON *.* TO' replication'@'%' IDENTIFIED BY 'password';

