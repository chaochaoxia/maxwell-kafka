-- Run this against the local MySQL as a user that can create accounts, for example:
--   mysql -uroot -p < mysql/init-maxwell-user.sql
--
-- Maxwell runs in Docker, so it is NOT a localhost client. The account must be 'maxwell'@'%'.

CREATE DATABASE IF NOT EXISTS demo;

CREATE USER IF NOT EXISTS 'maxwell'@'%' IDENTIFIED BY 'maxwell';
GRANT ALL ON maxwell.* TO 'maxwell'@'%';
GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'maxwell'@'%';
FLUSH PRIVILEGES;

-- Maxwell only works when the binary log is on and the format is ROW.
SHOW VARIABLES WHERE Variable_name IN ('log_bin', 'binlog_format', 'server_id', 'gtid_mode');
