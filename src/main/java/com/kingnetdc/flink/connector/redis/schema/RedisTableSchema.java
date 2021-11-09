package com.kingnetdc.flink.connector.redis.schema;

import com.kingnetdc.flink.connector.redis.util.TypeUtil;
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

	private final List<ColumnInfo> columnInfos;

	private ColumnInfo keyColumn;

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
					redisTableSchema.setKeyColumn(itemName, itemType, fieldIndex);
				}
				redisTableSchema.addColumn(itemName, itemType, fieldIndex, isKey);
				fieldIndex ++;
			} else {
				throw new IllegalArgumentException("Unsupported field type '" + fieldType + "' for redis");
			}
		}

		return redisTableSchema;
	}

	public List<ColumnInfo> getColumnInfos() {
		return columnInfos;
	}

	public void setKeyColumn(String name, DataType type, int index) {
		if (!TypeUtil.isSupportedType(type.getLogicalType())) {
			throw new IllegalArgumentException("Unsupported class type found " + type + ".");
		}
		if (keyColumn != null) {
			throw new UnsupportedOperationException("key column only support once time");
		}
		this.keyColumn = new ColumnInfo(name, type, index, true);
	}

	public ColumnInfo getKeyColumn() {
		return keyColumn;
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
