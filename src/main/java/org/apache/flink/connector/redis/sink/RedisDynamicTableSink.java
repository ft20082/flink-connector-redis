package org.apache.flink.connector.redis.sink;

import org.apache.flink.connector.redis.base.RedisConfig;
import org.apache.flink.connector.redis.base.SinkConfig;
import org.apache.flink.connector.redis.schema.RedisTableSchema;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.redis.base.Constants;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.types.RowKind;

public class RedisDynamicTableSink implements DynamicTableSink {

	private final ReadableConfig config;

	private final RedisTableSchema redisTableSchema;

	public RedisDynamicTableSink(ReadableConfig config, RedisTableSchema redisTableSchema) {
		this.config = config;
		this.redisTableSchema = redisTableSchema;
	}

	@Override
	public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
		ChangelogMode.Builder builder = ChangelogMode.newBuilder();
		for (RowKind kind : requestedMode.getContainedKinds()) {
			if (kind != RowKind.UPDATE_BEFORE) {
				builder.addContainedKind(kind);
			}
		}
		return builder.build();
	}

	@Override
	public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
		// redis config
		RedisConfig redisConfig = RedisConfig.fromConfig(config);
		// sink config
		SinkConfig sinkConfig = SinkConfig.fromConfig(config);
		RedisSinkFunction sinkFunction = new RedisSinkFunction(redisConfig, sinkConfig, redisTableSchema);
		return SinkFunctionProvider.of(sinkFunction, sinkConfig.getSinkParallelism());
	}

	@Override
	public DynamicTableSink copy() {
		return new RedisDynamicTableSink(config, redisTableSchema);
	}

	@Override
	public String asSummaryString() {
		return Constants.CONNECTOR_TYPE;
	}
}
