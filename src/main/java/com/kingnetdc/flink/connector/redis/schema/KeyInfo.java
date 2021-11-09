package com.kingnetdc.flink.connector.redis.schema;

import org.apache.flink.table.types.DataType;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class KeyInfo implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * redis key field name
	 */
	private final String[] keyName;

	/**
	 * redis key type, will convert to string
	 */
	private final DataType[] dataType;

	/**
	 * redis key index
	 */
	private final int[] keyIndex;

	/**
	 * field index set
	 */
	private final HashSet<Integer> keyIndexSet;

	public KeyInfo(String[] keyName, DataType[] dataType, int[] keyIndex) {
		this.keyName = keyName;
		this.dataType = dataType;
		this.keyIndex = keyIndex;
		keyIndexSet = new HashSet<>(keyIndex.length);
		for (int i = 0; i < keyIndex.length; i ++) {
			keyIndexSet.add(keyIndex[i]);
		}
	}

	public String[] getKeyName() {
		return keyName;
	}

	public DataType[] getDataType() {
		return dataType;
	}

	public int[] getKeyIndex() {
		return keyIndex;
	}

	public Set<Integer> getKeyIndexSet() {
		return keyIndexSet;
	}
}
