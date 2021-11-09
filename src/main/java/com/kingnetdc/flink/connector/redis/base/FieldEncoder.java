package com.kingnetdc.flink.connector.redis.base;

import org.apache.flink.table.data.RowData;

@FunctionalInterface
public interface FieldEncoder {

	String encode(RowData rowData, int pos);

}
