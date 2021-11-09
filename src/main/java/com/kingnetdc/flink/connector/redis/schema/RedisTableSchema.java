package com.kingnetdc.flink.connector.redis.schema;

import com.kingnetdc.flink.connector.redis.util.TypeUtil;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

public class RedisTableSchema implements Serializable {

	private static final long serialVersionUID = 1L;

	private final List<ColumnInfo> columnInfos;

	private KeyInfo keyInfo;

	public RedisTableSchema() {
		columnInfos = new LinkedList<>();
	}

	public void addColumn(String name, DataType type, int index, boolean isKey) {
		if (!TypeUtil.isSupportedType(type.getLogicalType())) {
			throw new IllegalArgumentException("Unsupported class type found " + type + ".");
		}
		columnInfos.add(new ColumnInfo(name, type, index, isKey));
	}

	/**
	 * not support row type
	 * @param resolvedSchema resolved schemas
	 * @return
	 */
	public static RedisTableSchema fromResolvedSchema(ResolvedSchema resolvedSchema) {
		RedisTableSchema redisTableSchema = new RedisTableSchema();
		UniqueConstraint primaryKey = resolvedSchema.getPrimaryKey()
				.orElseThrow(() -> new IllegalArgumentException("flink redis connector primary key should not be null"));
		List<String> keyColumns = primaryKey.getColumns();
		String[] keyNameArr = new String[keyColumns.size()];
		DataType[] keyTypeArr = new DataType[keyColumns.size()];
		int[] keyIndexArr = new int[keyColumns.size()];
		RowType rowType = (RowType) resolvedSchema.toPhysicalRowDataType().getLogicalType();
		int keyIndex = 0;
		int fieldIndex = 0;
		for (RowType.RowField field : rowType.getFields()) {
			LogicalType fieldType = field.getType();
			if (fieldType.getChildren().size() == 0) {
				String itemName = field.getName();
				DataType itemType = fromLogicalToDataType(fieldType);
				boolean isKey = keyColumns.contains(field.getName());
				if (isKey) {
					keyNameArr[keyIndex] = itemName;
					keyTypeArr[keyIndex] = itemType;
					keyIndexArr[keyIndex] = fieldIndex;
					keyIndex ++;
				}
				redisTableSchema.addColumn(itemName, itemType, fieldIndex, isKey);
				fieldIndex ++;
			} else {
				throw new IllegalArgumentException("Unsupported field type '" + fieldType + "' for redis");
			}
		}
		redisTableSchema.setKeyInfo(keyNameArr, keyTypeArr, keyIndexArr);
		return redisTableSchema;
	}

	public int[] getKeyIndex() {
		return keyInfo.getKeyIndex();
	}

	/**
	 * support primary key and unique key
	 * @param keyName key name string array
	 * @param type key type array
	 * @param keyIndex key index array
	 */
	public void setKeyInfo(String[] keyName, DataType[] type, int[] keyIndex) {
		if (keyName.length != type.length || type.length != keyIndex.length || keyName.length != keyIndex.length) {
			throw new IllegalArgumentException("key fields size should be equal.");
		}
		for (int i = 0; i < keyName.length; i ++) {
			Preconditions.checkNotNull(keyName[i], "key field name");
			Preconditions.checkNotNull(type[i], "key data type ");
			if (!TypeUtil.isSupportedType(type[i].getLogicalType())) {
				throw new IllegalArgumentException("Unsupported class type found " + type + ".");
			}
		}
		if (keyInfo != null) {
			throw new IllegalArgumentException("key can't be set multiple times.");
		}
		keyInfo = new KeyInfo(keyName, type, keyIndex);
	}

	public List<ColumnInfo> getColumnInfos() {
		return columnInfos;
	}

	public List<ColumnInfo> getKeyColumns() {
		LinkedList<ColumnInfo> ret = new LinkedList<>();
		columnInfos.forEach(item -> {
			if (item.isKey()) {
				ret.add(item);
			}
		});
		return ret;
	}

	public List<ColumnInfo> getNonKeyColumns() {
		LinkedList<ColumnInfo> ret = new LinkedList<>();
		columnInfos.forEach(item -> {
			if (!item.isKey()) {
				ret.add(item);
			}
		});
		return ret;
	}
}
