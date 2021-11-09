package com.kingnetdc.flink.connector.redis.schema;

import com.kingnetdc.flink.connector.redis.base.FieldDecoder;
import com.kingnetdc.flink.connector.redis.base.FieldEncoder;
import com.kingnetdc.flink.connector.redis.base.RedisConfig;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;


import java.math.BigDecimal;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;


public class RedisSerde {

	private static final int MIN_TIMESTAMP_PRECISION = 0;
	private static final int MAX_TIMESTAMP_PRECISION = 3;

	private static final int MIN_TIME_PRECISION = 0;
	private static final int MAX_TIME_PRECISION = 3;

	private static final String REDIS_KEY_SPLIT = ":::";

	/**
	 * use | split redis key
	 * @param redisTableSchema redis schema
	 * @param rowData row data
	 * @return
	 */
	public static String convertRowDataToKeyString(RedisTableSchema redisTableSchema,
												   RedisConfig redisConfig, RowData rowData) {
		List<ColumnInfo> keyColumn = redisTableSchema.getKeyColumns();
		StringBuilder sb = new StringBuilder();
		String keyPre = redisConfig.getKeyPre();
		String keyConcatString = redisConfig.getKeyConcatString();
		sb.append(keyPre);
		for (ColumnInfo columnInfo : keyColumn) {
			sb.append(keyConcatString).append(createFieldEncoder(columnInfo.getFieldType().getLogicalType())
					.encode(rowData, columnInfo.getKeyIndex()));
		}
		return sb.toString();
	}

	/**
	 * convert row data to Redis HashMap, use column name as key, data as value, except key fields
	 * @param rowData row data
	 * @return
	 */
	public static Map<String, String> convertRowDataToMap(RedisTableSchema redisTableSchema, RowData rowData) {
		Map<String, String> ret = new HashMap<>(rowData.getArity());
		List<ColumnInfo> nonKeyColumn = redisTableSchema.getNonKeyColumns();
		for (ColumnInfo columnInfo : nonKeyColumn) {
			String key = columnInfo.getKeyName();
			String value = createFieldEncoder(columnInfo.getFieldType().getLogicalType())
					.encode(rowData, columnInfo.getKeyIndex());
			ret.put(key, value);
		}
		return ret;
	}

	public static FieldDecoder createFieldDecoder(LogicalType fieldType) {
		switch (fieldType.getTypeRoot()) {
			case CHAR:
			case VARCHAR:
				return StringData::fromString;
			case BOOLEAN:
				return Boolean::valueOf;
			case BINARY:
			case VARBINARY:
				return value -> Base64.getDecoder().decode(value);
			case DECIMAL:
				DecimalType decimalType = (DecimalType) fieldType;
				final int precision = decimalType.getPrecision();
				final int scale = decimalType.getScale();
				return value -> {
					BigDecimal decimal = new BigDecimal(value);
					return DecimalData.fromBigDecimal(decimal, precision, scale);
				};
			case TINYINT:
				return value -> (byte) value.charAt(0);
			case SMALLINT:
				return Short::valueOf;
			case INTEGER:
			case DATE:
			case INTERVAL_YEAR_MONTH:
				return Integer::valueOf;
			case TIME_WITHOUT_TIME_ZONE:
				final int timePrecision = getPrecision(fieldType);
				if (timePrecision < MIN_TIME_PRECISION || timePrecision > MAX_TIME_PRECISION) {
					throw new UnsupportedOperationException(String.format("The precision %s of Time type is out of range [%s, %s]",
							timePrecision, MIN_TIME_PRECISION, MAX_TIME_PRECISION));
				}
				return Integer::valueOf;
			case BIGINT:
			case INTERVAL_DAY_TIME:
				return Long::valueOf;
			case FLOAT:
				return Float::valueOf;
			case DOUBLE:
				return Double::valueOf;
			case TIMESTAMP_WITHOUT_TIME_ZONE:
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				final int timestampPrecision = getPrecision(fieldType);
				if (timestampPrecision < MIN_TIMESTAMP_PRECISION || timestampPrecision > MAX_TIMESTAMP_PRECISION) {
					throw new UnsupportedOperationException(String.format("The precision %s of Timestamp is out of " +
							"range [%s, %s]", timestampPrecision, MIN_TIMESTAMP_PRECISION, MAX_TIMESTAMP_PRECISION));
				}
				return value -> {
					long milliseconds = Long.valueOf(value);
					return TimestampData.fromEpochMillis(milliseconds);
				};
			default:
				throw new UnsupportedOperationException("Unsupported type: " + fieldType);
		}
	}

	public static FieldEncoder createFieldEncoder(LogicalType fieldType) {
		switch (fieldType.getTypeRoot()) {
			case CHAR:
			case VARCHAR:
				return (rowData, pos) -> rowData.getString(pos).toString();
			case BOOLEAN:
				return (rowData, pos) -> String.valueOf(rowData.getBoolean(pos));
			case BINARY:
			case VARBINARY:
				return (rowData, pos) -> Base64.getEncoder().encodeToString(rowData.getBinary(pos));
			case DECIMAL:
				DecimalType decimalType = (DecimalType) fieldType;
				final int precision = decimalType.getPrecision();
				final int scale = decimalType.getScale();
				return (rowData, pos) -> {
					BigDecimal decimal = rowData.getDecimal(pos, precision, scale).toBigDecimal();
					return decimal.toString();
				};
			case TINYINT:
				return (rowData, pos) -> String.valueOf(rowData.getByte(pos));
			case SMALLINT:
				return (rowData, pos) -> String.valueOf(rowData.getString(pos));
			case INTEGER:
			case DATE:
			case INTERVAL_YEAR_MONTH:
				return (rowData, pos) -> String.valueOf(rowData.getInt(pos));
			case TIME_WITHOUT_TIME_ZONE:
				final int timePrecision = getPrecision(fieldType);
				if (timePrecision < MIN_TIME_PRECISION || timePrecision > MAX_TIME_PRECISION) {
					throw new UnsupportedOperationException(String.format("The precision %s of Time type is out of range [%s, %s]",
							timePrecision, MIN_TIME_PRECISION, MAX_TIME_PRECISION));
				}
				return (rowData, pos) -> String.valueOf(rowData.getInt(pos));
			case BIGINT:
			case INTERVAL_DAY_TIME:
				return (rowData, pos) -> String.valueOf(rowData.getLong(pos));
			case FLOAT:
				return (rowData, pos) -> String.valueOf(rowData.getFloat(pos));
			case DOUBLE:
				return (rowData, pos) -> String.valueOf(rowData.getDouble(pos));
			case TIMESTAMP_WITHOUT_TIME_ZONE:
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				final int timestampPrecision = getPrecision(fieldType);
				if (timestampPrecision < MIN_TIMESTAMP_PRECISION || timestampPrecision > MAX_TIMESTAMP_PRECISION) {
					throw new UnsupportedOperationException(String.format("The precision %s of Timestamp is out of " +
							"range [%s, %s]", timestampPrecision, MIN_TIMESTAMP_PRECISION, MAX_TIMESTAMP_PRECISION));
				}
				return (rowData, pos) -> {
					long millis = rowData.getTimestamp(pos, timestampPrecision).getMillisecond();
					return String.valueOf(millis);
				};
			default:
				throw new UnsupportedOperationException("Unsupported type: " + fieldType);
		}
	}

}
