package com.kingnetdc.flink.connector.redis.source;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.kingnetdc.flink.connector.redis.base.RedisConfig;
import com.kingnetdc.flink.connector.redis.base.SourceConfig;
import com.kingnetdc.flink.connector.redis.schema.RedisSerde;
import com.kingnetdc.flink.connector.redis.schema.RedisTableSchema;
import com.kingnetdc.flink.connector.redis.util.Common;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RedisLookupFunction extends TableFunction<RowData> {

	private static final Logger log = LoggerFactory.getLogger(RedisLookupFunction.class);

	private final RedisConfig redisConfig;

	private final SourceConfig sourceConfig;

	private final RedisTableSchema redisTableSchema;

	private transient GenericRowData rowData;

	private transient Cache<Object, RowData> cache;

	private transient JedisPool jedisPool;

	public RedisLookupFunction(RedisConfig redisConfig, SourceConfig sourceConfig, RedisTableSchema redisTableSchema) {
		this.redisConfig = redisConfig;
		this.sourceConfig = sourceConfig;
		this.redisTableSchema = redisTableSchema;
	}

	@Override
	public void open(FunctionContext context) throws Exception {
		log.info("start open ...");
		super.open(context);
		try {
			cache = sourceConfig.getLookupCacheMaxRows() > 0 &&  sourceConfig.getLookupCacheTtl() > 0 ?
					CacheBuilder.newBuilder().recordStats()
							.expireAfterWrite(sourceConfig.getLookupCacheTtl(), TimeUnit.SECONDS)
							.maximumSize(sourceConfig.getLookupCacheMaxRows()).build() : null;
			if (cache != null) {
				context.getMetricGroup().gauge("lookupCacheHitRate", (Gauge<Double>) () -> cache.stats().hitRate());
			}
			GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
			poolConfig.setMaxTotal(redisConfig.getPoolSize());
			poolConfig.setTestOnBorrow(redisConfig.getTestOnBorrow());
			jedisPool = new JedisPool(poolConfig, redisConfig.getHost(), redisConfig.getPort(),
					redisConfig.getTimeout(), redisConfig.getPassword(), redisConfig.getDb());
		} catch (Exception e) {
			log.error("redis source open error.", e);
			throw new RuntimeException("redis source open error.", e);
		}
		this.rowData = new GenericRowData(1);
		log.info("end open ...");
	}

	public void eval(Object key) throws IOException {
		if (cache != null) {
			RowData cacheRet = cache.getIfPresent(key);
			if (cacheRet != null) {
				collect(cacheRet);
				return;
			}
		}
		rowData.setField(0, key);
		String stringKey = RedisSerde.createFieldEncoder(
				redisTableSchema.getKeyField().getFieldType().getLogicalType()).encode(rowData, 0);
		int sleepTime = 500;
		for (int i = 0; i < sourceConfig.getLookupMaxRetries(); i ++) {
			Jedis jedis = null;
			try {
				jedis = jedisPool.getResource();
				Map<String, String> map = new HashMap<>();
				switch (redisConfig.getMode()) {
					case HASH:
						map = jedis.hgetAll(stringKey);
						break;
					case STRING:
						String str = jedis.get(stringKey);
						if (str != null && str.length() > 0) {
							map = Common.stringToMap(str);
						}
						break;
				}
				// convert map to row data fields
				if (map != null && map.size() > 0) {
					RowData ret = RedisSerde.convertMapToRowData(redisTableSchema, map);
					collect(ret);
					if (cache != null) {
						cache.put(key, ret);
					}
				}
				break;
			} catch (Exception e) {
				log.warn("redis operate error", e);
				Common.sleep(sleepTime * 2);
				if (i >= sourceConfig.getLookupMaxRetries()) {
					throw new RuntimeException("redis read error", e);
				}
			} finally {
				Common.closeJedis(jedis);
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
		if (cache != null) {
			cache.cleanUp();
		}
	}

}
