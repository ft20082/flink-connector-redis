package org.apache.flink.connector.redis.base;

import org.apache.flink.connector.redis.table.RedisConnectorOptions;
import org.apache.flink.configuration.ReadableConfig;

import java.io.Serializable;

public class SourceConfig implements Serializable {

	private final int lookupCacheMaxRows;

	private final int lookupCacheTtl;

	private final int lookupMaxRetries;

	public SourceConfig(int lookupCacheMaxRows, int lookupCacheTtl, int lookupMaxRetries) {
		this.lookupCacheMaxRows = lookupCacheMaxRows;
		this.lookupCacheTtl = lookupCacheTtl;
		this.lookupMaxRetries = lookupMaxRetries;
	}

	public int getLookupCacheMaxRows() {
		return lookupCacheMaxRows;
	}

	public int getLookupCacheTtl() {
		return lookupCacheTtl;
	}

	public int getLookupMaxRetries() {
		return lookupMaxRetries;
	}

	public static SourceConfig fromConfig(ReadableConfig config) {
		int lookupCacheMaxRows = config.get(RedisConnectorOptions.LOOKUP_CACHE_MAX_ROWS);
		int lookupCacheTtl = config.get(RedisConnectorOptions.LOOKUP_CACHE_TTL);
		int lookupMaxRetries = config.get(RedisConnectorOptions.LOOKUP_MAX_RETRIES);
		return new SourceConfig(lookupCacheMaxRows, lookupCacheTtl, lookupMaxRetries);
	}
}
