# flink-connector-redis

flink connector redis for flink sql

# maven dependency
maven repo
```xml
<repositories>
    <repository>
        <id>kingnetdc-public</id>
        <url>http://mirrors.kyhub.cn/repository/kingnetdc-maven-releases-group/</url>
    </repository>
    <repository>
        <id>maven-snapshots</id>
        <url>http://mirrors.kyhub.cn/repository/kingnetdc-maven-snapshots-group/</url>
    </repository>
</repositories>
```

maven dependency
```xml
<dependency>
    <groupId>com.kingnetdc.flink</groupId>
    <artifactId>flink-connector-redis</artifactId>
    <version>1.14.0-SNAPSHOT</version>
</dependency>
```

# how to use
```sql
CREATE TABLE redis_table (
    `key` STRING,
    `map_key_sid` INT,
    `map_key_install_date` STRING,
    PRIMARY KEY (`key`) NOT ENFORCED 
) WITH (
    'connector' = 'redis',
    'host' = '127.0.0.1'
);

-- 
INSERT INTO  redis_table
SELECT `key`, `map_key_sid`, `map_key_install_date` FROM T
```

# connector options
| 字段 | Required | 默认值  | 类型 | 说明 |
| ---- | ---- | ---- | ---- | ---- |
| connector | required  | (none) | String | `redis` |
| host | required  | (none) | String | Redis IP |
| port | optional | 6379 | Integer | Redis 端口 |
| password | optional | null | String | 如果没有设置，则为 null  |
| db | optional | 0 | Integer | 默认使用 db0 |
| pool-size | optional | 10 | Integer | 连接池大小 |
| timeout | optional | 3000  | Integer | 连接超时时间，单位 ms，默认 1s |
| test-on-borrow | optional | false | Boolean | 测试连接是否有效 |
| mode | optional | hash | String | redis 操作模式，使用 `hash` 或 `kv` |
| sink.parallelism | optional | (none) | Integer | 默认并行度 |
| sink.key-ttl | optional | 0s | Integer | 设置的 key 的超时时间，单位秒 |
| sink.max-retry | optional | 3 | Integer | 设置写入的重试次数 |
| lookup.cache.max-rows | optional | 10000 | Integer | lookup 缓存大小 |
| lookup.cache.ttl | optional | 0 | Integer | 缓存过期时间 |
| lookup.max-retries | optional | 3 | Integer | lookup 失败重试次数 |

> mode 对应的存储模式，如果是 `hash` 则按照字段存储，如果是 `kv` 则使用 json 存储

# Data Type Mapping
| Flink Sql Type | Redis conversion |
| ---- | ---- |
| CHAR | Store as string |
| VARCHAR | Store as string |
| STRING | Store as string |
| BOOLEAN | String toString(boolean val) <br/> boolean toBoolean(String str) |
| BINARY | String toString(byte[] val) <br/> byte[] toBytes(String str), Store as Base64 encoder string |
| VARBINARY | String toString(byte[] val) <br/> byte[] toBytes(String str), Store as Base64 encoder string |
| DECIMAL | String toString(BigDecimal val) <br/> BigDecimal toDecimal(String str) |
| TINYINT | String toString(byte val) <br/> byte toByte(String str), Store as Base64 encoder string |
| SMALLINT | String toString(short val) <br/> short toShort(String str) |
| INT | String toString(int val) <br/> int toInt(String str) |
| BIGINT | String toString(long val) <br/> long toLong(String str) |
| FLOAT | String toString(float val) <br/> float toFloat(String str) |
| DOUBLE | String toString(double val) <br/> double toDouble(String str) |
| DATE | Store the string type: '2021-10-10' |
| TIME | Store the string type: '10:10' |
| TIMESTAMP | Store the milliseconds since epoch as long value |
| ARRAY | Not supported  |
| MAP | Not supported |
| MULTISET | Not supported |
| ROW | Not supported |



