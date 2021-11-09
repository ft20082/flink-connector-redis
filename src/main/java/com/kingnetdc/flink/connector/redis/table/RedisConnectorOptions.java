package com.kingnetdc.flink.connector.redis.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class RedisConnectorOptions {

	public static final ConfigOption<String> HOST = ConfigOptions.key("host")
			.stringType().noDefaultValue().withDescription("redis host ip");

	public static final ConfigOption<Integer> PORT = ConfigOptions.key("port")
			.intType().defaultValue(6379).withDescription("redis port");

	public static final ConfigOption<String> PASSWORD = ConfigOptions.key("password")
			.stringType().defaultValue(null).withDescription("redis password, default is null");

	public static final ConfigOption<Integer> DB = ConfigOptions.key("db")
			.intType().defaultValue(0).withDescription("redis db, default is 0");

	public static final ConfigOption<Integer> POOL_SIZE = ConfigOptions.key("pool-size")
			.intType().defaultValue(10).withDescription("redis connector pool size, default is 10");

	public static final ConfigOption<Integer> TIMEOUT = ConfigOptions.key("timeout")
			.intType().defaultValue(3000).withDescription("connector timeout, unit is ms, default 3s");

	public static final ConfigOption<Boolean> TEST_ON_BORROW = ConfigOptions.key("test-on-borrow")
			.booleanType().defaultValue(false).withDescription("redis test on borrow, default is false");

	public static final ConfigOption<String> KEY_CONCAT_STRING = ConfigOptions.key("key-concat-string")
			.stringType().defaultValue(":").withDescription("redis key concat string, default is :");

	public static final ConfigOption<String> KEY_PRE = ConfigOptions.key("key-pre")
			.stringType().defaultValue("").withDescription("redis key pre");

	public static final ConfigOption<Integer> SINK_PARALLELISM = ConfigOptions.key("sink.parallelism")
			.intType().noDefaultValue().withDescription("sink parallelism, default use job parallelism");

	public static final ConfigOption<Long> SINK_KEY_TTL = ConfigOptions.key("sink.key-ttl")
			.longType().defaultValue(0L).withDescription("sink key ttl, default is 0");

	public static final ConfigOption<Integer> SINK_MAX_RETRY = ConfigOptions.key("sink.max-retry")
			.intType().defaultValue(3).withDescription("sink max retry, default is 3");

	public static final ConfigOption<Boolean> LOOKUP_ASYNC = ConfigOptions.key("lookup.async")
			.booleanType().defaultValue(false).withDescription("whether to set async lookup");

	public static final ConfigOption<Integer> LOOKUP_CACHE_MAX_ROWS = ConfigOptions.key("lookup.cache.max-rows")
			.intType().defaultValue(10000).withDescription("lookup cache size");

	public static final ConfigOption<Integer> LOOKUP_CACHE_TTL = ConfigOptions.key("lookup.cache.ttl")
			.intType().defaultValue(0).withDescription("cache expire time, unit is second");

	public static final ConfigOption<Integer> LOOKUP_MAX_RETRIES = ConfigOptions.key("lookup.max-retries")
			.intType().defaultValue(3).withDescription("lookup retries time when failed");

}
