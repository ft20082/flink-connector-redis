package org.apache.flink.connector.redis;

import org.apache.flink.connector.redis.schema.RedisTableSchema;
import org.apache.flink.connector.redis.sink.RedisDynamicTableSink;
import org.apache.flink.connector.redis.source.RedisDynamicTableSource;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.connector.redis.base.Constants.CONNECTOR_TYPE;
import static org.apache.flink.connector.redis.table.RedisConnectorOptions.DB;
import static org.apache.flink.connector.redis.table.RedisConnectorOptions.HOST;
import static org.apache.flink.connector.redis.table.RedisConnectorOptions.LOOKUP_CACHE_MAX_ROWS;
import static org.apache.flink.connector.redis.table.RedisConnectorOptions.LOOKUP_CACHE_TTL;
import static org.apache.flink.connector.redis.table.RedisConnectorOptions.LOOKUP_MAX_RETRIES;
import static org.apache.flink.connector.redis.table.RedisConnectorOptions.MODE;
import static org.apache.flink.connector.redis.table.RedisConnectorOptions.PASSWORD;
import static org.apache.flink.connector.redis.table.RedisConnectorOptions.POOL_SIZE;
import static org.apache.flink.connector.redis.table.RedisConnectorOptions.PORT;
import static org.apache.flink.connector.redis.table.RedisConnectorOptions.SINK_KEY_TTL;
import static org.apache.flink.connector.redis.table.RedisConnectorOptions.SINK_MAX_RETRY;
import static org.apache.flink.connector.redis.table.RedisConnectorOptions.SINK_PARALLELISM;
import static org.apache.flink.connector.redis.table.RedisConnectorOptions.TEST_ON_BORROW;
import static org.apache.flink.connector.redis.table.RedisConnectorOptions.TIMEOUT;
import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;

public class RedisDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

	private static final String IDENTIFIER = CONNECTOR_TYPE;

	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);
		helper.validate();
		ReadableConfig tableOptions = helper.getOptions();
		ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
		RedisTableSchema redisTableSchema = RedisTableSchema.fromResolvedSchema(resolvedSchema);
		return new RedisDynamicTableSink(tableOptions, redisTableSchema);
	}

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		FactoryUtil.TableFactoryHelper helper = createTableFactoryHelper(this, context);
		helper.validate();
		ReadableConfig tableOptions = helper.getOptions();
		ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
		RedisTableSchema redisTableSchema = RedisTableSchema.fromResolvedSchema(resolvedSchema);
		return new RedisDynamicTableSource(tableOptions, redisTableSchema);
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		Set<ConfigOption<?>> set = new HashSet<>();
		set.add(HOST);
		return set;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> set = new HashSet<>();
		set.add(PORT);
		set.add(PASSWORD);
		set.add(DB);
		set.add(POOL_SIZE);
		set.add(TIMEOUT);
		set.add(TEST_ON_BORROW);
		set.add(MODE);
		set.add(SINK_PARALLELISM);
		set.add(SINK_KEY_TTL);
		set.add(SINK_MAX_RETRY);
		set.add(LOOKUP_CACHE_MAX_ROWS);
		set.add(LOOKUP_CACHE_TTL);
		set.add(LOOKUP_MAX_RETRIES);
		return set;
	}

}
