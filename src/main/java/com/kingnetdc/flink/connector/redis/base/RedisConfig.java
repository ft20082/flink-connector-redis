package com.kingnetdc.flink.connector.redis.base;

import com.kingnetdc.flink.connector.redis.table.RedisConnectorOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

public class RedisConfig implements Serializable {

	private String host;

	private int port;

	private String password;

	private int db;

	private int poolSize;

	private int timeout;

	private boolean testOnBorrow;

	private RedisMode mode;

	public RedisConfig(String host, int port, String password, int db, int poolSize, int timeout,
					   boolean testOnBorrow, String mode) {
		this.host = host;
		this.port = port;
		this.password = password;
		this.db = db;
		this.poolSize = poolSize;
		this.timeout = timeout;
		this.testOnBorrow = testOnBorrow;
		RedisMode redisMode = RedisMode.of(mode);
		Preconditions.checkNotNull(redisMode, "mode only support `hash` and `kv`");
		this.mode = redisMode;
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	public String getPassword() {
		return password;
	}

	public int getDb() {
		return db;
	}

	public int getPoolSize() {
		return poolSize;
	}

	public int getTimeout() {
		return timeout;
	}

	public boolean getTestOnBorrow() {
		return testOnBorrow;
	}

	public RedisMode getMode() {
		return mode;
	}

	public static RedisConfig fromConfig(ReadableConfig config) {
		String host = config.get(RedisConnectorOptions.HOST);
		int port = config.get(RedisConnectorOptions.PORT);
		String password = config.get(RedisConnectorOptions.PASSWORD);
		int db = config.get(RedisConnectorOptions.DB);
		int poolSize = config.get(RedisConnectorOptions.POOL_SIZE);
		int timeout = config.get(RedisConnectorOptions.TIMEOUT);
		boolean testOnBorrow = config.get(RedisConnectorOptions.TEST_ON_BORROW);
		String mode = config.get(RedisConnectorOptions.MODE);
		return new RedisConfig(host, port, password, db, poolSize, timeout, testOnBorrow, mode);
	}
}
