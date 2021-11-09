package com.kingnetdc.flink.connector.redis;

import com.kingnetdc.flink.connector.redis.schema.RedisTableSchema;
import com.kingnetdc.flink.connector.redis.sink.RedisDynamicTableSink;
import com.kingnetdc.flink.connector.redis.source.RedisDynamicTableSource;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.kingnetdc.flink.connector.redis.base.Constants.CONNECTOR_TYPE;
import static com.kingnetdc.flink.connector.redis.table.RedisConnectorOptions.DB;
import static com.kingnetdc.flink.connector.redis.table.RedisConnectorOptions.HOST;
import static com.kingnetdc.flink.connector.redis.table.RedisConnectorOptions.KEY_CONCAT_STRING;
import static com.kingnetdc.flink.connector.redis.table.RedisConnectorOptions.KEY_PRE;
import static com.kingnetdc.flink.connector.redis.table.RedisConnectorOptions.LOOKUP_ASYNC;
import static com.kingnetdc.flink.connector.redis.table.RedisConnectorOptions.LOOKUP_CACHE_MAX_ROWS;
import static com.kingnetdc.flink.connector.redis.table.RedisConnectorOptions.LOOKUP_CACHE_TTL;
import static com.kingnetdc.flink.connector.redis.table.RedisConnectorOptions.LOOKUP_MAX_RETRIES;
import static com.kingnetdc.flink.connector.redis.table.RedisConnectorOptions.PASSWORD;
import static com.kingnetdc.flink.connector.redis.table.RedisConnectorOptions.POOL_SIZE;
import static com.kingnetdc.flink.connector.redis.table.RedisConnectorOptions.PORT;
import static com.kingnetdc.flink.connector.redis.table.RedisConnectorOptions.SINK_KEY_TTL;
import static com.kingnetdc.flink.connector.redis.table.RedisConnectorOptions.SINK_MAX_RETRY;
import static com.kingnetdc.flink.connector.redis.table.RedisConnectorOptions.SINK_PARALLELISM;
import static com.kingnetdc.flink.connector.redis.table.RedisConnectorOptions.TEST_ON_BORROW;
import static com.kingnetdc.flink.connector.redis.table.RedisConnectorOptions.TIMEOUT;
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
		return new RedisDynamicTableSource();
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
		set.add(KEY_CONCAT_STRING);
		set.add(KEY_PRE);
		set.add(SINK_PARALLELISM);
		set.add(SINK_KEY_TTL);
		set.add(SINK_MAX_RETRY);
		set.add(LOOKUP_ASYNC);
		set.add(LOOKUP_CACHE_MAX_ROWS);
		set.add(LOOKUP_CACHE_TTL);
		set.add(LOOKUP_MAX_RETRIES);
		return set;
	}

}
