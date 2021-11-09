package com.kingnetdc.flink.connector.redis.sink;

import com.kingnetdc.flink.connector.redis.base.RedisConfig;
import com.kingnetdc.flink.connector.redis.base.SinkConfig;
import com.kingnetdc.flink.connector.redis.schema.RedisTableSchema;
import com.kingnetdc.flink.connector.redis.util.Common;
import com.kingnetdc.flink.connector.redis.schema.RedisSerde;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Map;

public class RedisSinkFunction extends RichSinkFunction<RowData> implements CheckpointedFunction {

	private static final Logger log = LoggerFactory.getLogger(RedisSinkFunction.class);

	private final RedisConfig redisConfig;

	private final SinkConfig sinkConfig;

	private final RedisTableSchema redisTableSchema;

	private transient JedisPool jedisPool;

	public RedisSinkFunction(RedisConfig redisConfig, SinkConfig sinkConfig, RedisTableSchema redisTableSchema) {
		this.redisConfig = redisConfig;
		this.sinkConfig = sinkConfig;
		this.redisTableSchema = redisTableSchema;
	}

	public Jedis getJedis() {
		return jedisPool.getResource();
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		if (jedisPool == null) {
			try {
				GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
				poolConfig.setMaxTotal(redisConfig.getPoolSize());
				poolConfig.setTestOnBorrow(redisConfig.getTestOnBorrow());
				jedisPool = new JedisPool(poolConfig, redisConfig.getHost(), redisConfig.getPort(),
						redisConfig.getTimeout(), redisConfig.getPassword(), redisConfig.getDb());
			} catch (Exception e) {
				log.error("connect to redis error", e);
				throw new RuntimeException("connector to redis error", e);
			}
		}
		log.info("redis connector open ...");
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {
		// nothing to do
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		// nothing to do
	}

	@Override
	public void invoke(RowData value, Context context) throws Exception {
		System.out.println("A1 " + value);
		int maxRetry = sinkConfig.getSinkMaxRetry();
		int sleepTime = 500;
		for (int i = 0; i < maxRetry; i ++) {
			Jedis jedis = null;
			try {
				jedis = getJedis();
				RowKind kind = value.getRowKind();
				String key = RedisSerde.convertRowDataToKeyString(redisTableSchema, redisConfig, value);
				System.out.println("A2 " + key);
				if (kind == RowKind.INSERT || kind == RowKind.UPDATE_AFTER) {
					Map<String, String> hash = RedisSerde.convertRowDataToMap(redisTableSchema, value);
					System.out.println("A3 " + hash);
					String ret = jedis.hmset(key, hash);
					System.out.println("A4 " + ret);
					if (sinkConfig.getSinkKeyTtl() > 0) {
						jedis.expire(key, sinkConfig.getSinkKeyTtl());
					}
				} else {
					jedis.del(key);
				}
				break;
			} catch (Exception e) {
				System.out.println("exception: " + e);
				Common.sleep(sleepTime * 2);
				if (i >= maxRetry) {
					throw new RuntimeException("redis write error", e);
				}
			} finally {
				closeJedis(jedis);
			}
		}
	}

	@Override
	public void close() throws Exception {
		super.close();
		if (jedisPool != null) {
			try {
				jedisPool.close();
			} catch (Exception e) {
				log.warn("close jedis pool error", e);
			}
			jedisPool = null;
		}
	}

	public void closeJedis(Jedis jedis) {
		try {
			if (jedis != null) {
				jedis.close();
			}
		} catch (Exception e) {
			log.info("close jedis error", e);
		}
	}
}