package org.apache.flink.connector.redis.base;

@FunctionalInterface
public interface FieldDecoder {

	Object decode(String value);

}
