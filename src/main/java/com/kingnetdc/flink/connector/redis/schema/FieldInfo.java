package com.kingnetdc.flink.connector.redis.schema;

import org.apache.flink.table.types.DataType;

import java.io.Serializable;

public class FieldInfo implements Serializable {

	private final String name;

	private final DataType fieldType;

	private final int index;

	private final boolean isKey;

	public FieldInfo(String name, DataType fieldType, int index, boolean isKey) {
		this.name = name;
		this.fieldType = fieldType;
		this.index = index;
		this.isKey = isKey;
	}

	public String getName() {
		return name;
	}

	public DataType getFieldType() {
		return fieldType;
	}

	public int getIndex() {
		return index;
	}

	public boolean isKey() {
		return isKey;
	}

}
