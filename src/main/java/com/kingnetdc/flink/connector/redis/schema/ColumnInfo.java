package com.kingnetdc.flink.connector.redis.schema;

import org.apache.flink.table.types.DataType;

import java.io.Serializable;

public class ColumnInfo implements Serializable {

	private final String keyName;

	private final DataType fieldType;

	private final int keyIndex;

	private final boolean isKey;

	public ColumnInfo(String keyName, DataType fieldType, int keyIndex, boolean isKey) {
		this.keyName = keyName;
		this.fieldType = fieldType;
		this.keyIndex = keyIndex;
		this.isKey = isKey;
	}

	public String getKeyName() {
		return keyName;
	}

	public DataType getFieldType() {
		return fieldType;
	}

	public int getKeyIndex() {
		return keyIndex;
	}

	public boolean isKey() {
		return isKey;
	}

}
