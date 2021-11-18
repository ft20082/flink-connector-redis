package org.apache.flink.connector.redis.source;

import org.apache.flink.connector.redis.base.RedisConfig;
import org.apache.flink.connector.redis.base.SourceConfig;
import org.apache.flink.connector.redis.schema.RedisTableSchema;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.redis.base.Constants;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;

public class RedisDynamicTableSource implements LookupTableSource {

	private final ReadableConfig config;

	private final RedisTableSchema redisTableSchema;

	public RedisDynamicTableSource(ReadableConfig config, RedisTableSchema redisTableSchema) {
		this.config = config;
		this.redisTableSchema = redisTableSchema;
	}

	@Override
	public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
		// redis config
		RedisConfig redisConfig = RedisConfig.fromConfig(config);
		SourceConfig sourceConfig = SourceConfig.fromConfig(config);
		return TableFunctionProvider.of(new RedisLookupFunction(redisConfig, sourceConfig, redisTableSchema));
	}

	@Override
	public DynamicTableSource copy() {
		return new RedisDynamicTableSource(config, redisTableSchema);
	}

	@Override
	public String asSummaryString() {
		return Constants.CONNECTOR_TYPE;
	}

}
