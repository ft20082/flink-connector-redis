package org.apache.flink.connector.redis.schema;

import org.apache.flink.connector.redis.util.TypeUtil;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

public class RedisTableSchema implements Serializable {

	private static final long serialVersionUID = 1L;

	private final List<FieldInfo> fieldInfos;

	private FieldInfo keyField;

	public RedisTableSchema() {
		fieldInfos = new LinkedList<>();
	}

	public void addColumn(String name, DataType type, int index, boolean isKey) {
		if (!TypeUtil.isSupportedType(type.getLogicalType())) {
			throw new IllegalArgumentException("Unsupported class type found " + type + ".");
		}
		fieldInfos.add(new FieldInfo(name, type, index, isKey));
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
		if (keyColumns.size() != 1) {
			throw new IllegalArgumentException("flink redis connector key should be one field");
		}
		RowType rowType = (RowType) resolvedSchema.toPhysicalRowDataType().getLogicalType();
		int fieldIndex = 0;
		for (RowType.RowField field : rowType.getFields()) {
			LogicalType fieldType = field.getType();
			if (fieldType.getChildren().size() == 0) {
				String itemName = field.getName();
				DataType itemType = fromLogicalToDataType(fieldType);
				boolean isKey = keyColumns.contains(field.getName());
				if (isKey) {
					redisTableSchema.setKeyField(itemName, itemType, fieldIndex);
				}
				redisTableSchema.addColumn(itemName, itemType, fieldIndex, isKey);
				fieldIndex ++;
			} else {
				throw new IllegalArgumentException("Unsupported field type '" + fieldType + "' for redis");
			}
		}

		return redisTableSchema;
	}

	public List<FieldInfo> getFieldInfos() {
		return fieldInfos;
	}

	public void setKeyField(String name, DataType type, int index) {
		if (!TypeUtil.isSupportedType(type.getLogicalType())) {
			throw new IllegalArgumentException("Unsupported class type found " + type + ".");
		}
		if (keyField != null) {
			throw new UnsupportedOperationException("key column only support once time");
		}
		this.keyField = new FieldInfo(name, type, index, true);
	}

	public FieldInfo getKeyField() {
		return keyField;
	}

	public List<FieldInfo> getNonKeyFields() {
		LinkedList<FieldInfo> ret = new LinkedList<>();
		fieldInfos.forEach(item -> {
			if (!item.isKey()) {
				ret.add(item);
			}
		});
		return ret;
	}
}
