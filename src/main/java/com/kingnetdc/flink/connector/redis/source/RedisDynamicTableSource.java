package com.kingnetdc.flink.connector.redis.source;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;

import static com.kingnetdc.flink.connector.redis.base.Constants.CONNECTOR_TYPE;

public class RedisDynamicTableSource implements ScanTableSource, LookupTableSource, SupportsProjectionPushDown {

	@Override
	public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
		return null;
	}

	@Override
	public ChangelogMode getChangelogMode() {
		return null;
	}

	@Override
	public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
		return null;
	}

	@Override
	public DynamicTableSource copy() {
		return null;
	}

	@Override
	public String asSummaryString() {
		return CONNECTOR_TYPE;
	}

	@Override
	public boolean supportsNestedProjection() {
		return false;
	}

	@Override
	public void applyProjection(int[][] projectedFields) {

	}
}
