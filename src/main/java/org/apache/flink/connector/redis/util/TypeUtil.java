package org.apache.flink.connector.redis.util;

import org.apache.flink.table.types.logical.LogicalType;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;

public class TypeUtil {

	private static final int MIN_TIME_PRECISION = 0;
	private static final int MAX_TIME_PRECISION = 3;

	private static final int MIN_TIMESTAMP_PRECISION = 0;
	private static final int MAX_TIMESTAMP_PRECISION = 3;



	public static boolean isSupportedType(LogicalType fieldType) {
		switch (fieldType.getTypeRoot()) {
			case CHAR:
			case VARCHAR:
			case BOOLEAN:
			case BINARY:
			case VARBINARY:
			case DECIMAL:
			case TINYINT:
			case SMALLINT:
			case INTEGER:
			case DATE:
			case INTERVAL_YEAR_MONTH:
			case BIGINT:
			case INTERVAL_DAY_TIME:
			case FLOAT:
			case DOUBLE:
				return true;
			case TIME_WITHOUT_TIME_ZONE:
				final int timePrecision = getPrecision(fieldType);
				if (timePrecision < MIN_TIME_PRECISION || timePrecision > MAX_TIME_PRECISION) {
					throw new UnsupportedOperationException(String.format("The precision %s of TIME is out of range " +
							"[%s, %s]", timePrecision, MIN_TIME_PRECISION, MAX_TIME_PRECISION));
				}
				return true;
			case TIMESTAMP_WITHOUT_TIME_ZONE:
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				final int timestampPrecision = getPrecision(fieldType);
				if (timestampPrecision < MIN_TIMESTAMP_PRECISION || timestampPrecision > MAX_TIMESTAMP_PRECISION) {
					throw new UnsupportedOperationException(String.format("The precision %s of TIME is out of range " +
							"[%s, %s]", timestampPrecision, MIN_TIMESTAMP_PRECISION, MAX_TIMESTAMP_PRECISION));
				}
				return true;
			case TIMESTAMP_WITH_TIME_ZONE:
			case ARRAY:
			case MULTISET:
			case MAP:
			case STRUCTURED_TYPE:
			case DISTINCT_TYPE:
			case RAW:
			case NULL:
			case SYMBOL:
			case UNRESOLVED:
				return false;
			default:
				throw new UnsupportedOperationException("Unsupported flink type" + fieldType);
		}
	}

}
