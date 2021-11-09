package com.kingnetdc.flink.connector.redis.base;

@FunctionalInterface
public interface FieldDecoder {

	Object decode(String value);

}
