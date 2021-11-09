package com.kingnetdc.flink.connector.redis.base;

import com.kingnetdc.flink.connector.redis.table.RedisConnectorOptions;
import org.apache.flink.configuration.ReadableConfig;

import java.io.Serializable;

public class SinkConfig implements Serializable {

	private final Integer sinkParallelism;

	private final long sinkKeyTtl;

	private final int sinkMaxRetry;

	public SinkConfig(Integer sinkParallelism, long sinkKeyTtl, int sinkMaxRetry) {
		this.sinkParallelism = sinkParallelism;
		this.sinkKeyTtl = sinkKeyTtl;
		this.sinkMaxRetry = sinkMaxRetry;
	}

	public Integer getSinkParallelism() {
		return sinkParallelism;
	}

	public long getSinkKeyTtl() {
		return sinkKeyTtl;
	}

	public int getSinkMaxRetry() {
		return sinkMaxRetry;
	}

	public static SinkConfig fromConfig(ReadableConfig config) {
		Integer sinkParallelism = config.get(RedisConnectorOptions.SINK_PARALLELISM);
		long sinkKeyTtl = config.get(RedisConnectorOptions.SINK_KEY_TTL);
		int sinkMaxRetry = config.get(RedisConnectorOptions.SINK_MAX_RETRY);
		return new SinkConfig(sinkParallelism, sinkKeyTtl, sinkMaxRetry);
	}
}
