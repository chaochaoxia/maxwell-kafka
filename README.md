# maxwell-kafka

把本机 MySQL 的 binlog 变成 Kafka 消息，并用一个 Go 程序打印出来。

Maxwell 读取 MySQL 的行格式 binlog，把每次插入、更新、删除写成 JSON，发到 Kafka 的 `maxwell` topic。Go 消费端只负责把收到的消息打到标准输出。Kafka 本身没有页面，仓库里附带了 [Kafbat UI](https://github.com/kafbat/kafka-ui)。

```text
MySQL binlog  →  Maxwell  →  Kafka topic `maxwell`  →  Go consumer
                                  ↑
                             http://localhost:8080
```

这是一套本地实验环境。账号密码写在仓库里，只适合自己的机器，不要拿去连线上数据库。

## 环境

- 本机已经在跑的 MySQL 8，监听 `3306`
- Docker Desktop
- Go 1.24+

Maxwell 官方镜像目前只有 amd64。Apple Silicon 上由 Docker Desktop 模拟运行。

## MySQL

Maxwell 只能读 **ROW** 格式的 binlog。用有建用户权限的账号执行：

```bash
mysql -uroot -p < mysql/init-maxwell-user.sql
```

脚本会创建 `demo` 库和复制账号 `maxwell` / `maxwell`，并打印这几项。需要的结果是：

| 变量 | 值 |
| --- | --- |
| `log_bin` | `ON` |
| `binlog_format` | `ROW` |
| `server_id` | 非 0 |

MySQL 8 官方安装默认就是这样。如果 `log_bin` 是 `OFF`，在 `/etc/my.cnf` 加上：

```ini
[mysqld]
server_id=1
log-bin=mysql-bin
binlog_format=ROW
```

然后重启 MySQL。Maxwell 跑在 Docker 里，连的是宿主机，所以账号必须是 `'maxwell'@'%'`。

## 启动 Kafka 和 Maxwell

```bash
docker compose up -d
```

| 服务 | 地址 | 谁在用 |
| --- | --- | --- |
| Kafka | `localhost:9092` | 宿主机上的 Go 程序 |
| Kafka | `broker:19092` | Maxwell 和 Web UI，走 Docker 网络 |
| Kafbat UI | http://localhost:8080 | 浏览器 |
| MySQL | `host.docker.internal:3306` | Maxwell |

页面路径：左侧 **Topics** → **maxwell** → **Messages**。

Maxwell 第一次连上后，会在 MySQL 里自己建 `maxwell` 库，用来记 binlog 位点。

## 消费消息

```bash
cd consumer
go run .
```

另开一个终端写几行数据：

```sql
CREATE TABLE demo.users (
  id INT PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(64)
);
INSERT INTO demo.users (name) VALUES ('ada');
UPDATE demo.users SET name = 'ada lovelace' WHERE id = 1;
```

消费端打印的一条插入大致是：

```json
{"database":"demo","table":"users","type":"insert","ts":1710000000,"data":{"id":1,"name":"ada"}}
```

`type` 一般是 `insert`、`update`、`delete`。`update` 里 `data` 是新值，`old` 只带变化前的列。

可以用环境变量换 broker、topic 和消费组：

| 变量 | 默认 |
| --- | --- |
| `KAFKA_BROKERS` | `localhost:9092` |
| `KAFKA_TOPIC` | `maxwell` |
| `KAFKA_GROUP` | `maxwell-printer` |

消费组打开了 `WatchPartitionChanges`。程序经常比 Maxwell 更早启动，当时 topic 还不存在，Kafka 会先分配 0 个分区。没有这个选项的话，topic 后来建出来，这个进程也不会重新入组，看起来就像“Kafka 里有消息，Go 却不消费”。

## 把已有数据补进 Kafka

binlog 里只有 Maxwell 启动之后的变更。表里原来的行要单独做一次 bootstrap：正在跑的 Maxwell 会 `SELECT` 整张表，把结果发到同一个 topic。

一张表：

```sql
INSERT INTO maxwell.bootstrap (database_name, table_name)
VALUES ('demo', 'users');
```

一个库里的全部普通表：

```sql
INSERT INTO maxwell.bootstrap (database_name, table_name)
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'demo'
  AND table_type = 'BASE TABLE';
```

不要把 `mysql`、`sys`、`information_schema`、`performance_schema`、`maxwell` 加进去。

每张表的消息顺序是：

1. `bootstrap-start`
2. 每一行一条 `bootstrap-insert`
3. bootstrap 期间这张表上新产生的 `insert` / `update` / `delete`
4. `bootstrap-complete`

进度：

```sql
SELECT id, database_name, table_name, is_complete, inserted_rows, total_rows
FROM maxwell.bootstrap
ORDER BY id DESC;
```

`is_complete = 1` 表示这张表扫完了。默认是异步 bootstrap，别的表的 binlog 不会被堵住。Maxwell 如果扫到一半重启，这张表会整表再扫一遍。

也可以在容器里用命令行触发同一件事：

```bash
docker exec maxwell bin/maxwell-bootstrap \
  --host host.docker.internal --user maxwell --password maxwell \
  --database demo --table users
```

## 停掉

```bash
docker compose down
```

Kafka 的数据在容器里，这条命令不会删掉。要连数据一起清掉：

```bash
docker compose down -v
```
