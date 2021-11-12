package com.kingnetdc.flink.connector.redis.source;

import com.kingnetdc.flink.connector.redis.base.RedisConfig;
import com.kingnetdc.flink.connector.redis.base.SourceConfig;
import com.kingnetdc.flink.connector.redis.schema.RedisTableSchema;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;

import static com.kingnetdc.flink.connector.redis.base.Constants.CONNECTOR_TYPE;

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
		return CONNECTOR_TYPE;
	}

}
