package com.kingnetdc.flink.connector.redis.base;

public enum RedisMode {

	HASH("hash"),

	STRING("string");

	private String type;

	RedisMode(String type) {
		this.type = type;
	}

	public static RedisMode of(String type) {
		for (RedisMode item : RedisMode.values()) {
			if (item.type.equalsIgnoreCase(type)) {
				return item;
			}
		}
		return null;
	}

}
